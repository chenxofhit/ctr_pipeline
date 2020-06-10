




class BaseDeviceIDDownloader(object):
    def __init__(self):
        super(BaseDeviceIDDownloader, self).__init__()
        self.init_oss()
        self.oss_prefix = ""  # init oss_prefix
        self.file_prefix = "result_imei"

        self.email_user = config.get_by_path("smtp_server/user")
        self.email_password = config.get_by_path("smtp_server/password")
        self.smtp_host = config.get_by_path("smtp_server/host")
        self.email_recipients = config.get_by_path(
            "app_wakeup/email_recipients"
        )

        self.tag_name = self.get_tag_name()
        self.uniq_tag_name = self.tag_name + "_uniq"
        self.orig_tag_name = self.tag_name + "_orig"

        self.expire_days_interval = 1
        self.day_diff = 0  # 处理向前多少天的数据，比如day_diff=1就是是10号处理9号的数据

    def get_tag_name(self) -> str:
        raise Exception("get_tag_name should be implemented in sub class")

    def init_args(self):
        self.arg_parser.add_argument(
            "--start_day",
            default=None,
            help="stat day, e.g. 2018-08-24",
            type=argparse_type.date,
        )

    @classmethod
    def task_adapter(cls, from_day=0, to_day=None):
        return DailyCli.task_adapter(from_day, to_day, key="start_day")

    def init_oss(self):
        oss_endpoint = config.get_by_path("app_wakeup/oss_end_point")
        oss_auth = oss2.Auth(
            config.get_by_path("app_wakeup/oss_access_key"),
            config.get_by_path("app_wakeup/oss_access_secret"),
        )
        self.oss_bucket = oss2.Bucket(
            oss_auth,
            oss_endpoint,
            config.get_by_path("app_wakeup/oss_bucket_name"),
        )

    def get_lastest_day_have_data(
        self, start_day: datetime.datetime, max_days: int = 5
    ) -> datetime.datetime:
        """
        有时会不出数据，为了不影响投放，需要向前找有数据的那一天
        :param start_day: 开始找的天，从这天向前找
        :param tag_name: device_id表中的tag
        :param max_days: 最多向前找的天，如果没找到，则返回None
        :return: 返回有数据的那一天， None表示在给定天数内没找到数据
        """
        check_day = start_day
        for i in range(max_days):
            sql = """
            select count(*) from device_id where day_idx = %d and tag = "%s" ;
            """ % (
                get_day_idx(check_day),
                self.tag_name,
            )

            with odps.execute_sql(sql).open_reader() as reader:
                for row in reader:
                    if row[0] > 0:
                        return check_day

            check_day -= datetime.timedelta(days=1)

        return None

    def uniq_id_in_odps(self, check_time: datetime.datetime):
        day_idx = get_day_idx(check_time)

        start_day_idx = get_day_idx(
            check_time - datetime.timedelta(days=self.expire_days_interval)
        )

        sql = """
        insert overwrite table device_id partition(day_idx=%d,tag="%s")
        select
            NULL, did_md5, "md5"
        from device_id
        where
            (day_idx = %d and tag = "%s") or (day_idx >= %d and tag = "%s")
        group by did_md5;
        """ % (
            day_idx,
            self.uniq_tag_name,
            day_idx,
            self.tag_name,
            start_day_idx,
            self.uniq_tag_name,
        )
        odps.execute_sql(sql)

    def insert_orig(self, check_time: datetime.datetime):
        end_day_idx = get_day_idx(check_time)
        start_day_idx = get_day_idx(
            check_time - datetime.timedelta(days=self.expire_days_interval)
        )

        sql = """
        insert overwrite table device_id partition(day_idx=%d,tag="%s")
        select
            did, did_md5, type
        from (
        select
            max(did) as did,
            max(did_md5) as did_md5,
            max(case when type = "md5" then NULL else type end) as type,
            max(case when tag = "%s" then 1 else 0 end) as id_exist
        from device_id
        where day_idx >= %d and day_idx <= %d and (tag = "xinyi_uniq" or tag = "%s")
        group by did_md5 ) t1
        where
            t1.id_exist = 1 and did is not NULL and did <> "" ;
        """ % (
            end_day_idx,
            self.orig_tag_name,
            self.tag_name,
            start_day_idx,
            end_day_idx,
            self.tag_name,
        )
        odps.execute_sql(sql)

    def uniq_origininal_files(self, data_path: str):
        cur_time = datetime.datetime.now() - datetime.timedelta(
            days=self.day_diff
        )
        for i in range(9):
            check_time = cur_time - datetime.timedelta(days=i)

            stat_day = check_time.strftime("%Y%m%d")
            data_files = self.get_files_with_prefix(
                data_path, self.file_prefix
            )

            original_files = []
            for f in data_files:
                if stat_day in f:
                    original_files.append(f)

            if len(original_files) <= 0:
                continue

            uniq_file = os.path.join(
                data_path,
                "%s.uniq.%s"
                % (self.file_prefix, check_time.strftime("%Y%m%d")),
            )
            if os.path.exists(uniq_file):
                continue

            cmd = "runiq -f digest '%s' > '%s'" % (
                "' '".join(original_files),
                uniq_file,
            )
            self.system(cmd)

    def get_files_with_prefix(
        self, dir_path: str, file_prefix: str
    ) -> List[str]:
        return [
            os.path.join(dir_path, f)
            for f in os.listdir(dir_path)
            if os.path.isfile(os.path.join(dir_path, f))
            and f.startswith(file_prefix)
        ]

    def get_data_dir(self) -> str:
        return config.get_dir_by_path("app_wakeup/data_dir", self.tag_name)

    def download_files_from_day(
        self, start_day: datetime.datetime
    ) -> List[str]:
        if not start_day:
            return []

        day = start_day
        cur_time = datetime.datetime.now()
        data_dir = self.get_data_dir()
        downloaded_files = []
        while day < cur_time:
            prefix = self.oss_prefix
            day_str = day.strftime("%Y%m%d")
            for object_info in oss2.ObjectIterator(
                self.oss_bucket, prefix=prefix
            ):
                if day_str not in object_info.key:
                    continue

                local_file_path = os.path.join(
                    data_dir, os.path.basename(object_info.key)
                )

                if os.path.exists(local_file_path):
                    continue

                downloaded_files.append(local_file_path)
                logger.info("download to %s" + local_file_path)
                self.oss_bucket.get_object_to_file(
                    object_info.key, local_file_path
                )

            day += datetime.timedelta(days=1)

        return downloaded_files

    def write_id_2_odps(
        self,
        user_did_file_path: str,
        check_time: datetime.datetime,
        id_type: str = "md5",
    ):
        day_idx = get_day_idx(check_time)
        line_count = 0
        odps_table = odps.get_table("device_id")
        data = []
        with open(user_did_file_path, "r") as dict_file:
            for line in dict_file:
                did_md5 = line.strip()
                if len(did_md5) <= 0:
                    continue

                line_count += 1
                data_item = (
                    None,
                    did_md5,
                    id_type,
                )

                data.append(data_item)

                if line_count % 1000000 == 0:
                    with odps_table.open_writer(
                        partition='day_idx=%d,tag="%s"'
                        % (day_idx, self.tag_name),
                        create_partition=True,
                    ) as writer:
                        writer.write(data)

                    data = []

            if data:
                with odps_table.open_writer(
                    partition='day_idx=%d,tag="%s"' % (day_idx, self.tag_name),
                    create_partition=True,
                ) as writer:
                    writer.write(data)

    def system(self, cmd: str):
        logger.info("command: %s" % cmd)
        os.system(cmd)

    def get_cache_expire_time(self) -> int:
        if self.expire_days_interval > 1:
            return config.get_by_path("app_wakeup/key_expire_time")
        else:
            return 86400 * self.expire_days_interval

    def process(self, check_time: datetime.datetime):
        process_date = self.get_lastest_day_have_data(check_time)
        if not process_date:
            email.send(
                self.email_user,
                self.email_password,
                self.email_recipients,
                "[严重错误]唤醒 %s 没有数据" % self.tag_name,
                "唤醒 %s 没有数据" % self.tag_name,
                self.smtp_host,
            )
            return
        elif process_date != check_time:
            email.send(
                self.email_user,
                self.email_password,
                self.email_recipients,
                "[错误]唤醒 %s 当天没有数据" % self.tag_name,
                "唤醒 %s 当天没有数据， 采用 %s 的数据"
                % (self.tag_name, process_date.strftime("%Y-%m-%d")),
                self.smtp_host,
            )

        self.insert_orig(process_date)
        self.uniq_id_in_odps(process_date)

        day_idx = get_day_idx(process_date)
        sql = """
        select
            (case when tag = "%s" then did else did_md5 end) as did
        from device_id
        where day_idx = %d and (tag = "%s" or tag = "%s")
        """ % (
            self.orig_tag_name,
            day_idx,
            self.orig_tag_name,
            self.tag_name,
        )

        key_expire_time = self.get_cache_expire_time()

        write_count = 0
        device_ids = []

        with odps.execute_sql(sql).open_reader() as reader:
            for row in reader:
                if not row[0]:
                    continue

                device_ids.append(row[0])

                write_count += 1
                if write_count % 100000 == 0:
                    # write device info into aerospike
                    as_client.device_write_2_aerospike(
                        self.tag_name, device_ids, 1, key_expire_time
                    )
                    device_ids = []

            # write device info into aerospike
            as_client.device_write_2_aerospike(
                self.tag_name, device_ids, 1, key_expire_time
            )

        self.process_md5_device_id(process_date)

    def process_md5_device_id(self, check_time: datetime.datetime):
        day_idx = get_day_idx(check_time)

        sql = """
        select
            did_md5
        from (select
            max(did_md5) as did_md5,
            max(case when tag = "%s" then 1 else 0 end) as id_exist,
            max(case when tag = "xinyi_md5" then 1 else 0 end) as xinyi_md5_exist
        from device_id
        where day_idx = %d and (tag = "xinyi_md5" or tag = "%s")
        group by did_md5 ) t1
        where
            t1.id_exist = 1 and xinyi_md5_exist = 1 ;
        """ % (
            self.tag_name,
            day_idx,
            self.tag_name,
        )

        write_count = 0
        device_ids = []
        key_expire_time = config.get_by_path("app_wakeup/key_expire_time")
        with odps.execute_sql(sql).open_reader() as reader:
            for row in reader:
                device_id = row.did_md5

                if not device_id:
                    continue

                device_ids.append(device_id)

                write_count += 1
                if write_count % 100000 == 0:
                    # write device info into aerospike
                    as_client.device_write_2_aerospike(
                        self.tag_name, device_ids, 1, key_expire_time
                    )
                    device_ids = []

            # write device info into aerospike
            as_client.device_write_2_aerospike(
                self.tag_name, device_ids, 1, key_expire_time
            )

    def get_uniq_file_path(
        self,
        data_dir: str,
        stat_time: datetime.datetime,
        check_exist: bool = True,
    ) -> str:
        first_day_uniq_file = None
        for i in range(3):
            file_day = stat_time - datetime.timedelta(days=i)
            uniq_file = os.path.join(
                data_dir,
                "%s.uniq.%s" % (self.file_prefix, file_day.strftime("%Y%m%d")),
            )

            if not first_day_uniq_file:
                first_day_uniq_file = uniq_file
            if check_exist:
                if not os.path.exists(uniq_file):
                    continue

            return uniq_file

        pathlib.Path(first_day_uniq_file).touch()

        return first_day_uniq_file

    def write_days_ago_device_id_2_cache(self, days_count: int):
        """
         当数据文件没有下载成功时，写入制定天数前到现在的id
         :param days_count:
         :return:
         """
        start_day = datetime.datetime.now() - datetime.timedelta(
            days=days_count
        )
        key_expire_time = self.get_cache_expire_time()
        write_helper = WriteDeviceID2CacheHelper()
        write_helper.write_device_id_2_cache(
            self.tag_name,
            "md5",
            self.tag_name,
            start_day,
            days_count,
            key_expire_time,
        )
        write_helper.write_device_id_2_cache(
            self.orig_tag_name,
            "imei",
            self.tag_name,
            start_day,
            days_count,
            key_expire_time,
        )

    def _run(self, parsed_args):
        if parsed_args.start_day:
            self.download_files_from_day(parsed_args.start_day)
            return

        cur_time = datetime.datetime.now()
        data_dir = self.get_data_dir()
        device_id_files = []
        while datetime.datetime.now() - cur_time < datetime.timedelta(hours=5):
            device_id_files = self.download_files_from_day(
                cur_time - datetime.timedelta(days=8)
            )

            if len(device_id_files) > 0:
                break

            time.sleep(300)

        logger.info(
            "base_device_id_downloader: device_id_file_num=%d"
            % len(device_id_files)
        )

        if len(device_id_files) <= 0:
            self.write_days_ago_device_id_2_cache(3)
            try:
                raise Exception("tag:%s no data files" % self.tag_name)
            except Exception as e:
                sentry_sdk.capture_exception(e)
                return

        self.uniq_origininal_files(data_dir)
        stat_time = cur_time - datetime.timedelta(days=self.day_diff)
        uniq_file = self.get_uniq_file_path(data_dir, stat_time)
        user_did_file_path = os.path.join(
            data_dir,
            "%s.%s.csv" % (self.tag_name, stat_time.strftime("%Y%m%d")),
        )
        os.rename(uniq_file, user_did_file_path)

        self.write_id_2_odps(user_did_file_path, stat_time)
        self.process(stat_time)




