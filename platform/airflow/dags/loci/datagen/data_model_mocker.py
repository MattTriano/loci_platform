import csv
import datetime
import random
from pathlib import Path

from faker import Faker


class DataFaker:
    def __init__(
        self,
        output_dir: Path,
        seed: int = 42,
        address_count: int = 250000,
        incremental: bool = False,
        update_percentage: float = 0.05,
        new_percentage: float = 0.02,
    ):
        self.output_dir = Path(output_dir)
        self.seed = seed
        self.address_count = address_count
        self.business_count = int((self.address_count * 0.01) // 1)
        self.partnership_count = self.business_count * 8
        self.incremental = incremental
        self.update_percentage = update_percentage
        self.new_percentage = new_percentage
        self.fake = Faker()
        Faker.seed(self.seed)
        random.seed(self.seed)
        self.timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d_%H%M%S")
        self.generate_data()

    def get_latest_files(self) -> dict | None:
        files = {
            "addresses": None,
            "businesses": None,
            "partnerships": None,
        }
        for file in self.output_dir.glob("addresses_*.csv"):
            if files["addresses"] is None or file.name > files["addresses"].name:
                files["addresses"] = file
        for file in self.output_dir.glob("businesses_*.csv"):
            if files["businesses"] is None or file.name > files["businesses"].name:
                files["businesses"] = file
        for file in self.output_dir.glob("trading_partnerships_*.csv"):
            if files["partnerships"] is None or file.name > files["partnerships"].name:
                files["partnerships"] = file
        if all(v is None for v in files.values()):
            return None
        return files

    def load_existing_data(self, filepath: Path) -> list:
        with open(filepath, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)

    def generate_data(self):
        print("\n" + "=" * 70)
        print("Business Network CSV Data Generator")
        print(f"Random Seed: {self.seed} (reproducible data)")
        print(f"Output Directory: {self.output_dir}")
        print(f"Timestamp: {self.timestamp}")
        if self.incremental:
            print(
                f"Mode: INCREMENTAL (update={self.update_percentage:.1%}, new={self.new_percentage:.1%})"
            )
        else:
            print("Mode: FULL")
        print("=" * 70 + "\n")
        self.output_dir.mkdir(exist_ok=True)
        if self.incremental:
            latest_files = self.get_latest_files()
            if latest_files is None:
                print("No existing files found. Generating full dataset...")
                self.incremental = False
            else:
                print("Found existing files. Generating incremental update...")
                self.generate_incremental_update(latest_files)
                return
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
        print(f"  - addresses_{self.timestamp}.csv ({self.address_count:,} rows)")
        print(f"  - businesses_{self.timestamp}.csv ({self.business_count:,} rows)")
        print(f"  - trading_partnerships_{self.timestamp}.csv ({self.partnership_count:,} rows)")
        print("  - DATASET_INFO.txt (summary)")
        print(
            f"\nTotal: {self.address_count + self.business_count + self.partnership_count:,} records"
        )
        print(f"Seed: {self.seed}")
        print("\n Data generation complete!")
        print("=" * 70 + "\n")

    def generate_incremental_update(self, latest_files: dict):
        addresses = self.load_existing_data(latest_files["addresses"])
        businesses = self.load_existing_data(latest_files["businesses"])
        partnerships = self.load_existing_data(latest_files["partnerships"])
        addr_count_init = len(addresses)
        addr_update_count = int(len(addresses) * self.update_percentage)
        addr_new_count = int(len(addresses) * self.new_percentage)
        biz_count_init = len(businesses)
        biz_update_count = int(len(businesses) * self.update_percentage)
        biz_new_count = int(len(businesses) * self.new_percentage)
        part_update_count = int(len(partnerships) * self.update_percentage)
        part_new_count = int(len(partnerships) * self.new_percentage)
        print(
            f"Addresses: {len(addresses):,} existing, {addr_update_count:,} updates, {addr_new_count:,} new"
        )
        print(
            f"Businesses: {len(businesses):,} existing, {biz_update_count:,} updates, {biz_new_count:,} new"
        )
        print(
            f"Partnerships: {len(partnerships):,} existing, {part_update_count:,} updates, {part_new_count:,} new\n"
        )
        self.update_addresses_csv(addresses, addr_update_count, addr_new_count)
        self.update_businesses_csv(
            businesses,
            biz_update_count,
            biz_new_count,
            addr_count_init + addr_new_count,
        )
        self.update_partnerships_csv(
            partnerships,
            part_update_count,
            part_new_count,
            biz_count_init + biz_new_count,
        )
        print("\n Incremental update complete!")
        print("=" * 70 + "\n")

    def generate_addresses_csv(self) -> None:
        filepath = self.output_dir.joinpath(f"addresses_{self.timestamp}.csv")
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
        print(f" Created {filepath} ({self.address_count:,} rows)")

    def update_addresses_csv(self, existing: list, update_count: int, new_count: int) -> None:
        filepath = self.output_dir.joinpath(f"addresses_{self.timestamp}.csv")
        print("Updating addresses file...")
        update_indices = random.sample(range(len(existing)), min(update_count, len(existing)))
        now = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        for idx in update_indices:
            existing[idx]["street_address"] = self.fake.street_address()
            existing[idx]["updated_at"] = now
        max_id = max(int(row["address_id"]) for row in existing)
        for i in range(1, new_count + 1):
            new_row = {
                "address_id": max_id + i,
                "street_address": self.fake.street_address(),
                "city": self.fake.city(),
                "state": self.fake.state(),
                "postal_code": self.fake.postcode(),
                "country": "United States",
                "created_at": now,
                "updated_at": now,
            }
            existing.append(new_row)
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=existing[0].keys())
            writer.writeheader()
            writer.writerows(existing)
        print(f" Created {filepath} ({len(existing):,} rows)")

    def generate_businesses_csv(self) -> None:
        filepath = self.output_dir.joinpath(f"businesses_{self.timestamp}.csv")
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
                    f"{company_name} Inc." if random.random() > 0.3 else f"{company_name} LLC",
                    f"{self.fake.random_int(10, 99)}-{self.fake.random_int(1000000, 9999999)}",
                    random.choice(industries),
                    self.fake.bs().capitalize(),
                    self.fake.company_email(),
                    self.fake.phone_number(),
                    self.fake.url(),
                    random.choice([10, 25, 50, 100, 250, 500, 1000, 5000, 10000]),
                    random.randint(100000, 50000000),
                    random.randint(1, self.address_count),
                    random.random() > 0.1,
                    datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]
                writer.writerow(row)
                if i % (self.address_count // 250) == 0:
                    print(
                        f"  Progress: {i:,}/{self.business_count:,} ({i / self.business_count * 100:.1f}%)"
                    )
        print(f" Created {filepath} ({self.business_count:,} rows)")

    def update_businesses_csv(
        self, existing: list, update_count: int, new_count: int, max_address_id: int
    ) -> None:
        filepath = self.output_dir.joinpath(f"businesses_{self.timestamp}.csv")
        print("Updating businesses file...")
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
        update_indices = random.sample(range(len(existing)), min(update_count, len(existing)))
        now = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        for idx in update_indices:
            existing[idx]["employee_count"] = random.choice(
                [10, 25, 50, 100, 250, 500, 1000, 5000, 10000]
            )
            existing[idx]["annual_revenue"] = random.randint(100000, 50000000)
            existing[idx]["is_active"] = random.random() > 0.1
            existing[idx]["updated_at"] = now
        max_id = max(int(row["business_id"]) for row in existing)
        for i in range(1, new_count + 1):
            company_name = self.fake.company()
            new_row = {
                "business_id": max_id + i,
                "name": company_name,
                "legal_name": f"{company_name} Inc."
                if random.random() > 0.3
                else f"{company_name} LLC",
                "tax_id": f"{self.fake.random_int(10, 99)}-{self.fake.random_int(1000000, 9999999)}",
                "industry": random.choice(industries),
                "description": self.fake.bs().capitalize(),
                "email": self.fake.company_email(),
                "phone": self.fake.phone_number(),
                "website": self.fake.url(),
                "employee_count": random.choice([10, 25, 50, 100, 250, 500, 1000, 5000, 10000]),
                "annual_revenue": random.randint(100000, 50000000),
                "address_id": random.randint(1, max_address_id),
                "is_active": random.random() > 0.1,
                "created_at": now,
                "updated_at": now,
            }
            existing.append(new_row)
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=existing[0].keys())
            writer.writeheader()
            writer.writerows(existing)
        print(f" Created {filepath} ({len(existing):,} rows)")

    def generate_partnerships_csv(self) -> None:
        filepath = self.output_dir.joinpath(f"trading_partnerships_{self.timestamp}.csv")
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
            partnerships_created = 0
            attempts = 0
            max_attempts = self.partnership_count * 3
            while partnerships_created < self.partnership_count and attempts < max_attempts:
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
                start_date = self.fake.date_time_between(start_date="-5y", end_date="now")
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
                if partnerships_created % 1000 == 0:
                    print(
                        f"  Progress: {partnerships_created:,}/{self.partnership_count:,} ({partnerships_created / self.partnership_count * 100:.1f}%)"
                    )
        print(f" Created {filepath} ({partnerships_created:,} rows)")

    def update_partnerships_csv(
        self, existing: list, update_count: int, new_count: int, max_business_id: int
    ) -> None:
        filepath = self.output_dir.joinpath(f"trading_partnerships_{self.timestamp}.csv")
        print("Updating partnerships file...")
        partnership_types = [
            "supplier",
            "customer",
            "partner",
            "distributor",
            "reseller",
            "affiliate",
        ]
        update_indices = random.sample(range(len(existing)), min(update_count, len(existing)))
        now = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        for idx in update_indices:
            existing[idx]["is_active"] = random.random() > 0.2
            existing[idx]["contract_value"] = (
                random.randint(10000, 5000000) if random.random() > 0.3 else ""
            )
            existing[idx]["updated_at"] = now
        max_id = max(int(row["partnership_id"]) for row in existing)
        used_pairs = {
            tuple(sorted([int(row["business1_id"]), int(row["business2_id"])])) for row in existing
        }
        partnerships_added = 0
        attempts = 0
        while partnerships_added < new_count and attempts < new_count * 10:
            attempts += 1
            business1_id = random.randint(1, max_business_id)
            business2_id = random.randint(1, max_business_id)
            if business1_id == business2_id:
                continue
            pair = tuple(sorted([business1_id, business2_id]))
            if pair in used_pairs:
                continue
            used_pairs.add(pair)
            is_active = random.random() > 0.2
            start_date = self.fake.date_time_between(start_date="-1y", end_date="now")
            new_row = {
                "partnership_id": max_id + partnerships_added + 1,
                "business1_id": business1_id,
                "business2_id": business2_id,
                "is_active": is_active,
                "partnership_type": random.choice(partnership_types),
                "start_date": start_date.isoformat(),
                "end_date": "",
                "contract_value": random.randint(10000, 5000000) if random.random() > 0.3 else "",
                "notes": self.fake.sentence() if random.random() > 0.5 else "",
                "created_at": now,
                "updated_at": now,
            }
            existing.append(new_row)
            partnerships_added += 1
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=existing[0].keys())
            writer.writeheader()
            writer.writerows(existing)
        print(f" Created {filepath} ({len(existing):,} rows)")

    def generate_summary_file(self, stats: dict) -> None:
        filepath = self.output_dir.joinpath("DATASET_INFO.txt")
        with open(filepath, "w") as f:
            f.write("=" * 70 + "\n")
            f.write("Business Network Mock Data - Dataset Summary\n")
            f.write("=" * 70 + "\n\n")
            f.write(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Timestamp: {self.timestamp}\n")
            f.write(f"Random Seed: {stats['seed']}\n")
            f.write(f"Output Directory: {self.output_dir}\n\n")
            f.write("Dataset Contents:\n")
            f.write("-" * 70 + "\n")
            f.write(f"Addresses:           {stats['addresses']:,} rows\n")
            f.write(f"Businesses:          {stats['businesses']:,} rows\n")
            f.write(f"Trading Partnerships: {stats['partnerships']:,} rows\n")
            f.write(f"Total Records:       {sum(stats.values()) - 1:,} rows\n\n")
            f.write("Files Generated:\n")
            f.write("-" * 70 + "\n")
            f.write(f"1. addresses_{self.timestamp}.csv - Business location data\n")
            f.write(f"2. businesses_{self.timestamp}.csv - Business profile information\n")
            f.write(f"3. trading_partnerships_{self.timestamp}.csv - Business relationships\n\n")
            f.write("Reproducibility:\n")
            f.write("-" * 70 + "\n")
            f.write(f"To regenerate this exact dataset, use seed: {stats['seed']}\n")
            f.write(f"  python generate_csv_data.py {stats['seed']}\n\n")
        print(f" Created {filepath}")
