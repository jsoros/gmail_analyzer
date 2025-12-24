import time
import sys
from datetime import datetime
from progress.spinner import Spinner
try:
    from ascii_graph import Pyasciigraph
    from termgraph.termgraph import chart, calendar_heatmap
except ImportError:
    print("Error: Required visualization packages not found.")
    print("Please run: pipenv install ascii-graph termgraph")
    sys.exit(1)
import agate
import warnings
import concurrent.futures
from threading import Event
import termtables

from src import helpers
from src.processor import Processor


class Metrics:
    def __init__(self, args):
        # Ignore warnings about SSL connections
        warnings.simplefilter("ignore", ResourceWarning)

        self.processor = Processor(
            query=args.get("query"),
            max_retry_rounds=args.get("max_retry_rounds"),
        )
        self.user_id = args["user"]
        self.resultsLimit = args["top"]
        self.inactive_days = args["inactive"]
        self.pull_data = args.get("pull_data", False)
        self.refresh_data = args.get("refresh_data", False)
        self.analyze_only = args.get("analyze_only", False)
        self.export_csv = args.get("export_csv")
        self.table = None

    def _load_table(self, event):
        table = agate.Table.from_object(list(self.processor.messagesQueue))

        event.set()

        self.table = table

        return

    def _analyze_senders(self, event):
        data = (
            self.table.pivot("fields/from")
            .where(lambda row: row["fields/from"] is not None)
            .order_by("Count", reverse=True)
            .limit(self.resultsLimit)
        )

        _values = data.columns.values()

        data_keys = list(_values[0].values())
        data_count = [[i] for i in list(map(int, list(_values[1].values())))]

        event.set()

        print(f"\n\n{helpers.h1_icn} Senders (top {self.resultsLimit})\n")
        
        try:
            args = {
                "stacked": False,
                "width": 55,
                "no_labels": False,
                "format": "{:<,d}",
                "suffix": "",
                "vertical": False,
                "different_scale": False,
            }
            
            chart(colors=[94], data=data_count, args=args, labels=data_keys)
        except Exception as e:
            print(f"Note: Could not display chart. Using simple output instead. Error: {str(e)}")
            # Print a simple table if chart function fails
            for i in range(len(data_keys)):
                if i < len(data_count):
                    print(f"{data_keys[i]}: {data_count[i][0]:,}")

    def _analyze_count(self, event):
        # Average emails per day
        total = self.table.aggregate([("total", agate.Count())])["total"]
        total_senders = (
            self.table.distinct("fields/from")
            .select("fields/from")
            .aggregate([("total", agate.Count())])["total"]
        )

        if total == 0:
            first_email_date = ""
            last_email_date = None
        else:
            date_data = self.table.where(
                lambda row: row["fields/date"] is not None
            ).compute(
                [
                    (
                        "reduce_to_datetime",
                        agate.Formula(
                            agate.DateTime(datetime_format="%Y-%m-%d %H:%M:%S"),
                            lambda row: helpers.reduce_to_datetime(row["fields/date"]),
                        ),
                    )
                ]
            )
            first_email_date = (
                date_data.order_by("reduce_to_datetime")
                .limit(1)
                .columns["fields/date"]
                .values()[0]
            )
            last_email_date = (
                date_data.order_by("reduce_to_datetime", reverse=True)
                .limit(1)
                .columns["fields/date"]
                .values()[0]
            )
        event.set()

        metrics = [
            ["Total emails", total],
            ["Senders", total_senders],
            ["First Email Date", first_email_date],
        ]

        if last_email_date:
            date_delta = helpers.convert_date(last_email_date) - helpers.convert_date(
                first_email_date
            )
            avg_email_per_day = total / date_delta.days
            metrics.append(["Avg. Emails/Day", f"{avg_email_per_day:.2f}"])

        print(f"\n\n{helpers.h1_icn} Stats\n")
        print(termtables.to_string(metrics))

    def _analyze_inactive_senders(self, event):
        """Analyze senders who haven't sent emails in X days"""
        if self.inactive_days <= 0:
            event.set()
            return
            
        # Get current date for comparison
        current_date = datetime.now()
        
        # Process the data to get the last email date for each sender
        sender_last_dates = {}
        
        # Filter out rows with no sender or date
        filtered_table = self.table.where(
            lambda row: row["fields/from"] is not None and row["fields/date"] is not None
        )
        
        # Convert dates to datetime objects for comparison
        date_table = filtered_table.compute([
            (
                "datetime",
                agate.Formula(
                    agate.DateTime(datetime_format="%Y-%m-%d %H:%M:%S"),
                    lambda row: helpers.reduce_to_datetime(row["fields/date"]),
                ),
            )
        ])
        
        # Group by sender and find the most recent email
        for row in date_table.rows:
            sender = row["fields/from"]
            date_obj = helpers.convert_date(row["fields/date"])
            
            if sender not in sender_last_dates or date_obj > sender_last_dates[sender]["date"]:
                sender_last_dates[sender] = {
                    "date": date_obj,
                    "date_str": row["fields/date"]
                }
        
        # Find senders inactive for more than X days
        inactive_senders = []
        for sender, data in sender_last_dates.items():
            days_since = (current_date - data["date"]).days
            if days_since > self.inactive_days:
                inactive_senders.append({
                    "sender": sender,
                    "last_email_date": data["date_str"],
                    "days_since": days_since
                })
        
        # Sort by days_since in descending order
        inactive_senders.sort(key=lambda x: x["days_since"], reverse=True)
        
        # Limit to top results
        inactive_senders = inactive_senders[:self.resultsLimit]
        
        event.set()
        
        if inactive_senders:
            print(f"\n\n{helpers.h1_icn} Senders inactive for more than {self.inactive_days} days\n")
            
            # Prepare data for table display
            table_data = []
            for sender in inactive_senders:
                table_data.append([
                    sender["sender"],
                    sender["last_email_date"],
                    f"{sender['days_since']} days"
                ])
            
            headers = ["Sender", "Last Email Date", "Days Since"]
            print(termtables.to_string(table_data, header=headers))
        else:
            print(f"\n\n{helpers.h1_icn} No senders inactive for more than {self.inactive_days} days found")
    
    def _analyze_date(self, event):
        table = self.table.where(lambda row: row["fields/date"] is not None).compute(
            [
                (
                    "reduce_to_date",
                    agate.Formula(
                        agate.Text(),
                        lambda row: helpers.reduce_to_date(row["fields/date"]),
                    ),
                ),
                (
                    "reduce_to_year",
                    agate.Formula(
                        agate.Number(),
                        lambda row: helpers.reduce_to_year(row["fields/date"]),
                    ),
                ),
                (
                    "reduce_to_time",
                    agate.Formula(
                        agate.Number(),
                        lambda row: helpers.reduce_to_time(row["fields/date"]),
                    ),
                ),
            ]
        )

        years = table.distinct("reduce_to_year").columns["reduce_to_year"].values()

        _data = {}

        for year in years:
            _data[year] = (
                table.where(lambda row: row["reduce_to_year"] == year)
                .select("reduce_to_date")
                .pivot("reduce_to_date")
                .order_by("reduce_to_date")
            )

        event.set()

        print(f"\n\n{helpers.h1_icn} Date\n")

        for year in years:
            data_keys = list(_data[year].columns["reduce_to_date"].values())
            _counts = list(map(int, list(_data[year].columns["Count"].values())))
            _sum = sum(_counts)
            data_count = [[i] for i in _counts]

            args = {"color": False, "custom_tick": False, "start_dt": f"{year}-01-01"}

            print(f"\n{helpers.h2_icn} Year {year} ({_sum:,} emails)\n")
            
            try:
                calendar_heatmap(data=data_count, args=args, labels=data_keys)
            except Exception as e:
                print(f"Note: Could not display calendar heatmap. Showing simple output instead. Error: {str(e)}")
                # Show top 10 dates with most emails
                sorted_dates = sorted(zip(data_keys, [c[0] for c in data_count]), key=lambda x: x[1], reverse=True)
                for date, count in sorted_dates[:10]:
                    print(f"{date}: {count:,} emails")

    def analyse(self):
        """
        read from the messages queue, and generate:
        1. Counter for From field
        2. Counter for Time field (by hour)
        """

        # {'id': '16f39fe119ee8427', 'labels': ['UNREAD', 'CATEGORY_UPDATES', 'INBOX'], 'fields': {'from': 'Coursera <no-reply@t.mail.coursera.org>', 'date': 'Tue, 24 Dec 2019 22:13:09 +0000'}}

        with concurrent.futures.ThreadPoolExecutor() as executor:
            progress = Spinner(f"{helpers.loader_icn} Loading messages ")

            event = Event()

            future = executor.submit(self._load_table, event)

            while not event.isSet() and future.running():
                progress.next()
                time.sleep(0.1)

            progress.finish()

            progress = Spinner(f"{helpers.loader_icn} Analysing count ")

            event = Event()

            future = executor.submit(self._analyze_count, event)

            while not event.isSet() and future.running():
                progress.next()
                time.sleep(0.1)

            progress.finish()

            progress = Spinner(f"{helpers.loader_icn} Analysing senders ")

            event = Event()

            future = executor.submit(self._analyze_senders, event)

            while not event.isSet() and future.running():
                progress.next()
                time.sleep(0.1)

            progress.finish()

            progress = Spinner(f"{helpers.loader_icn} Analysing dates ")

            event = Event()

            future = executor.submit(self._analyze_date, event)

            while not event.isSet() and future.running():
                progress.next()
                time.sleep(0.1)

            progress.finish()
            
            # Only run inactive senders analysis if the threshold is set
            if self.inactive_days > 0:
                progress = Spinner(f"{helpers.loader_icn} Analysing inactive senders ")
                
                event = Event()
                
                future = executor.submit(self._analyze_inactive_senders, event)
                
                while not event.isSet() and future.running():
                    progress.next()
                    time.sleep(0.1)
                
                progress.finish()
            
            # Print completion message
            print("\nAnalysis complete!")

    def start(self):
        if self.analyze_only:
            if not self.processor.load_cached_metadata():
                print("No cached metadata found. Run with --pull-data first.")
                return
            if self.export_csv:
                self.processor.export_csv(self.export_csv)
            self.analyse()
            return

        force_refresh = self.refresh_data
        messages = self.processor.get_messages(force_refresh=force_refresh)

        self.processor.get_metadata(messages, force_refresh=force_refresh)

        if self.export_csv:
            self.processor.export_csv(self.export_csv)

        if self.pull_data or self.refresh_data:
            print("Data pull complete.")
            return

        self.analyse()
