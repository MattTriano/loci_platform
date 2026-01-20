import csv
import random
import datetime
from pathlib import Path

from faker import Faker


class DataFaker:
    def __init__(self, output_dir: Path, seed: int = 42, address_count: int = 250000):
        self.output_dir = Path(output_dir)
        self.seed = seed
        self.address_count = address_count
        self.business_count = int((self.address_count * 0.01) // 1)
        self.partnership_count = self.business_count * 8
        self.fake = Faker()
        Faker.seed(self.seed)
        random.seed(self.seed)
        self.generate_data()

    def generate_data(self):
        print("\n" + "=" * 70)
        print("Business Network CSV Data Generator")
        print(f"Random Seed: {self.seed} (reproducible data)")
        print(f"Output Directory: {self.output_dir}")
        print("=" * 70 + "\n")

        self.output_dir.mkdir(exist_ok=True)
        self.generate_addresses_csv()
        self.generate_businesses_csv()
        self.generate_partnerships_csv()
        stats = {
            "seed": self.seed,
            "addresses": self.address_count,
            "businesses": self.business_count,
            "partnerships": self.partnership_count,
        }
        self.generate_summary_file(stats)

        print("\n" + "=" * 70)
        print("Summary:")
        print("=" * 70)
        print(f"Output Directory: {self.output_dir}/")
        print(f"  - addresses.csv ({self.address_count:,} rows)")
        print(f"  - businesses.csv ({self.business_count:,} rows)")
        print(f"  - trading_partnerships.csv ({self.partnership_count:,} rows)")
        print("  - DATASET_INFO.txt (summary)")
        print(
            f"\nTotal: {self.address_count + self.business_count + self.partnership_count:,} records"
        )
        print(f"Seed: {self.seed}")
        print("\n✅ Data generation complete!")
        print(
            f"\nTo inspect: Open {self.output_dir}/addresses.csv in Excel or any CSV viewer"
        )
        print(f"To regenerate: python generate_csv_data.py {self.seed}")
        print("=" * 70 + "\n")

    def generate_addresses_csv(self) -> None:
        filepath = self.output_dir.joinpath("addresses.csv")
        print(f"Generating {self.address_count:,} addresses...")
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "address_id",
                    "street_address",
                    "city",
                    "state",
                    "postal_code",
                    "country",
                    "created_at",
                    "updated_at",
                ]
            )
            for i in range(1, self.address_count + 1):
                row = [
                    i,
                    self.fake.street_address(),
                    self.fake.city(),
                    self.fake.state(),
                    self.fake.postcode(),
                    "United States",
                    datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]
                writer.writerow(row)
                if i % (self.address_count // 25) == 0:
                    print(
                        f"  Progress: {i:,}/{self.address_count:,} ({i / self.address_count * 100:.1f}%)"
                    )
        print(f"✓ Created {filepath} ({self.address_count:,} rows)")

    def generate_businesses_csv(self) -> None:
        filepath = self.output_dir.joinpath("businesses.csv")
        print(f"Generating {self.business_count:,} businesses...")
        industries = [
            "Technology",
            "Manufacturing",
            "Retail",
            "Healthcare",
            "Finance",
            "Construction",
            "Food & Beverage",
            "Transportation",
            "Energy",
            "Telecommunications",
            "Consulting",
            "Real Estate",
            "Media & Entertainment",
            "Pharmaceuticals",
            "Aerospace",
        ]
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "business_id",
                    "name",
                    "legal_name",
                    "tax_id",
                    "industry",
                    "description",
                    "email",
                    "phone",
                    "website",
                    "employee_count",
                    "annual_revenue",
                    "address_id",
                    "is_active",
                    "created_at",
                    "updated_at",
                ]
            )
            for i in range(1, self.business_count + 1):
                company_name = self.fake.company()
                row = [
                    i,
                    company_name,
                    f"{company_name} Inc."
                    if random.random() > 0.3
                    else f"{company_name} LLC",
                    f"{self.fake.random_int(10, 99)}-{self.fake.random_int(1000000, 9999999)}",
                    random.choice(industries),
                    self.fake.bs().capitalize(),
                    self.fake.company_email(),
                    self.fake.phone_number(),
                    self.fake.url(),
                    random.choice([10, 25, 50, 100, 250, 500, 1000, 5000, 10000]),
                    random.randint(100000, 50000000),
                    random.randint(1, self.address_count),
                    random.random() > 0.1,  # 90% active
                    datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]
                writer.writerow(row)
                if i % (self.address_count // 250) == 0:
                    print(
                        f"  Progress: {i:,}/{self.business_count:,} ({i / self.business_count * 100:.1f}%)"
                    )
        print(f"✓ Created {filepath} ({self.business_count:,} rows)")

    def generate_partnerships_csv(self) -> None:
        filepath = self.output_dir.joinpath("trading_partnerships.csv")
        print(f"Generating {self.partnership_count:,} trading partnerships...")
        partnership_types = [
            "supplier",
            "customer",
            "partner",
            "distributor",
            "reseller",
            "affiliate",
        ]
        used_pairs = set()
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "partnership_id",
                    "business1_id",
                    "business2_id",
                    "is_active",
                    "partnership_type",
                    "start_date",
                    "end_date",
                    "contract_value",
                    "notes",
                    "created_at",
                    "updated_at",
                ]
            )
            batch_size = 1000
            partnerships_created = 0
            attempts = 0
            max_attempts = self.partnership_count * 3
            while (
                partnerships_created < self.partnership_count
                and attempts < max_attempts
            ):
                attempts += 1
                business1_id = random.randint(1, self.business_count)
                business2_id = random.randint(1, self.business_count)
                if business1_id == business2_id:
                    continue
                pair = tuple(sorted([business1_id, business2_id]))
                if pair in used_pairs:
                    continue
                used_pairs.add(pair)
                is_active = random.random() > 0.2
                start_date = self.fake.date_time_between(
                    start_date="-5y", end_date="now"
                )
                end_date = ""
                if not is_active:
                    end_date = self.fake.date_time_between(
                        start_date=start_date, end_date="now"
                    ).isoformat()
                row = [
                    partnerships_created + 1,
                    business1_id,
                    business2_id,
                    is_active,
                    random.choice(partnership_types),
                    start_date.isoformat(),
                    end_date,
                    random.randint(10000, 5000000) if random.random() > 0.3 else "",
                    self.fake.sentence() if random.random() > 0.5 else "",
                    datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]
                writer.writerow(row)
                partnerships_created += 1
                if partnerships_created % batch_size == 0:
                    print(
                        f"  Progress: {partnerships_created:,}/{self.partnership_count:,} "
                        f"({partnerships_created / self.partnership_count * 100:.1f}%)"
                    )
        print(f"✓ Created {filepath} ({partnerships_created:,} rows)")

    def generate_summary_file(self, stats: dict) -> None:
        filepath = self.output_dir.joinpath("DATASET_INFO.txt")
        with open(filepath, "w") as f:
            f.write("=" * 70 + "\n")
            f.write("Business Network Mock Data - Dataset Summary\n")
            f.write("=" * 70 + "\n\n")

            f.write(
                f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
            f.write(f"Random Seed: {stats['seed']}\n")
            f.write(f"Output Directory: {self.output_dir}\n\n")

            f.write("Dataset Contents:\n")
            f.write("-" * 70 + "\n")
            f.write(f"Addresses:           {stats['addresses']:,} rows\n")
            f.write(f"Businesses:          {stats['businesses']:,} rows\n")
            f.write(f"Trading Partnerships: {stats['partnerships']:,} rows\n")
            f.write(
                f"Total Records:       {sum(stats.values()) - 1:,} rows\n\n"
            )  # -1 to exclude seed

            f.write("Files Generated:\n")
            f.write("-" * 70 + "\n")
            f.write("1. addresses.csv - Business location data\n")
            f.write("2. businesses.csv - Business profile information\n")
            f.write("3. trading_partnerships.csv - Business relationships\n\n")

            f.write("CSV Format:\n")
            f.write("-" * 70 + "\n")
            f.write("- UTF-8 encoding\n")
            f.write("- Comma delimited\n")
            f.write("- Header row included\n")
            f.write("- Empty strings for NULL values\n\n")

            f.write("Relationships:\n")
            f.write("-" * 70 + "\n")
            f.write("- businesses.address_id → addresses.id\n")
            f.write("- trading_partnerships.business1_id → businesses.id\n")
            f.write("- trading_partnerships.business2_id → businesses.id\n\n")

            f.write("Next Steps:\n")
            f.write("-" * 70 + "\n")
            f.write("1. Inspect CSV files in any spreadsheet application\n")
            f.write("2. Use provided SQL scripts to create tables\n")
            f.write("3. Import CSVs into your database:\n")
            f.write("   - PostgreSQL: COPY command (see import_postgres.sql)\n")
            f.write("   - MySQL: LOAD DATA INFILE\n")
            f.write("   - SQLite: .import command\n")
            f.write("   - Python: pandas.read_csv() + to_sql()\n\n")

            f.write("Reproducibility:\n")
            f.write("-" * 70 + "\n")
            f.write(f"To regenerate this exact dataset, use seed: {stats['seed']}\n")
            f.write(f"  python generate_csv_data.py {stats['seed']}\n\n")
        print(f" Created {filepath}")
