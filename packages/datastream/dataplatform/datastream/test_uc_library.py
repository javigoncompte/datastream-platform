from typing import Optional
import polars as pl
from sqlmodel import SQLModel, Field, create_engine, Session
from databricks import sql
import os
from pathlib import Path


class DigimonModel(SQLModel, table=True):
    """SQLModel representing a Digimon from the DigiDB dataset."""
    
    __tablename__ = "digimon_catalog"
    
    # Primary key
    number: int = Field(primary_key=True, description="Unique identifier for the Digimon")
    
    # Core attributes
    digimon: str = Field(description="Name of the Digimon")
    stage: str = Field(description="Evolution stage (Baby, In-Training, Rookie, Champion, Ultimate, Mega, Ultra, Armor)")
    type: str = Field(description="Type classification (Vaccine, Virus, Data, Free)")
    attribute: str = Field(description="Elemental attribute (Fire, Water, Plant, etc.)")
    
    # Resource stats
    memory: int = Field(description="Memory usage")
    equip_slots: int = Field(description="Number of equipment slots")
    
    # Level 50 stats
    lv50_hp: int = Field(description="HP at level 50")
    lv50_sp: int = Field(description="SP at level 50")
    lv50_atk: int = Field(description="Attack at level 50")
    lv50_def: int = Field(description="Defense at level 50")
    lv50_int: int = Field(description="Intelligence at level 50")
    lv50_spd: int = Field(description="Speed at level 50")


class DigimonDataProcessor:
    """Handles reading Digimon data from CSV and writing to Unity Catalog."""
    
    def __init__(self, csv_path: str, unity_catalog_table: str = "main.default.digimon_catalog"):
        self.csv_path = csv_path
        self.unity_catalog_table = unity_catalog_table
        
    def read_csv_data(self) -> pl.DataFrame:
        """Read Digimon data from CSV using Polars."""
        print(f"Reading CSV data from: {self.csv_path}")
        
        # Read CSV with Polars
        df = pl.read_csv(self.csv_path)
        
        # Clean column names to match SQLModel fields
        df = df.rename({
            "Number": "number",
            "Digimon": "digimon", 
            "Stage": "stage",
            "Type": "type",
            "Attribute": "attribute",
            "Memory": "memory",
            "Equip Slots": "equip_slots",
            "Lv 50 HP": "lv50_hp",
            "Lv50 SP": "lv50_sp",
            "Lv50 Atk": "lv50_atk",
            "Lv50 Def": "lv50_def",
            "Lv50 Int": "lv50_int",
            "Lv50 Spd": "lv50_spd"
        })
        
        print(f"Successfully read {len(df)} records from CSV")
        return df
    
    def convert_to_sqlmodel_records(self, df: pl.DataFrame) -> list[DigimonModel]:
        """Convert Polars DataFrame to SQLModel records."""
        print("Converting DataFrame to SQLModel records...")
        
        records = []
        for row in df.iter_rows(named=True):
            digimon_record = DigimonModel(
                number=row["number"],
                digimon=row["digimon"],
                stage=row["stage"],
                type=row["type"],
                attribute=row["attribute"],
                memory=row["memory"],
                equip_slots=row["equip_slots"],
                lv50_hp=row["lv50_hp"],
                lv50_sp=row["lv50_sp"],
                lv50_atk=row["lv50_atk"],
                lv50_def=row["lv50_def"],
                lv50_int=row["lv50_int"],
                lv50_spd=row["lv50_spd"]
            )
            records.append(digimon_record)
            
        print(f"Successfully converted {len(records)} records to SQLModel")
        return records
    
    def write_to_unity_catalog(self, records: list[DigimonModel]) -> None:
        """Write SQLModel records to Unity Catalog."""
        print(f"Writing {len(records)} records to Unity Catalog table: {self.unity_catalog_table}")
        
        # Get Databricks connection parameters from environment variables
        # In a real scenario, these would be set as environment variables or fetched from secrets
        server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME", "your-workspace.cloud.databricks.com")
        http_path = os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/your-warehouse-id")
        access_token = os.getenv("DATABRICKS_ACCESS_TOKEN", "your-access-token")
        
        # Create DataFrame from SQLModel records for batch insert
        df_data = []
        for record in records:
            df_data.append({
                "number": record.number,
                "digimon": record.digimon,
                "stage": record.stage,
                "type": record.type,
                "attribute": record.attribute,
                "memory": record.memory,
                "equip_slots": record.equip_slots,
                "lv50_hp": record.lv50_hp,
                "lv50_sp": record.lv50_sp,
                "lv50_atk": record.lv50_atk,
                "lv50_def": record.lv50_def,
                "lv50_int": record.lv50_int,
                "lv50_spd": record.lv50_spd
            })
        
        # Convert back to Polars DataFrame for efficient Unity Catalog insertion
        df_to_write = pl.DataFrame(df_data)
        
        # Generate CREATE TABLE statement
        create_table_sql = self._generate_create_table_sql()
        
        # Generate INSERT statements (batch approach)
        insert_sql = self._generate_insert_sql(df_to_write)
        
        print("Generated SQL statements for Unity Catalog:")
        print(f"CREATE TABLE: {create_table_sql[:100]}...")
        print(f"INSERT statements generated for {len(df_to_write)} records")
        
        # Note: Actual execution would require valid Databricks credentials
        # This is a placeholder as requested - no actual execution due to missing secrets
        print("Note: SQL execution skipped due to missing Databricks credentials")
        print("In a real scenario, you would execute these statements using:")
        print("- Databricks SQL connector")
        print("- Unity Catalog REST API")
        print("- Databricks SDK")
        
    def _generate_create_table_sql(self) -> str:
        """Generate CREATE TABLE SQL for Unity Catalog."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self.unity_catalog_table} (
            number INT NOT NULL,
            digimon STRING NOT NULL,
            stage STRING NOT NULL,
            type STRING NOT NULL,
            attribute STRING NOT NULL,
            memory INT NOT NULL,
            equip_slots INT NOT NULL,
            lv50_hp INT NOT NULL,
            lv50_sp INT NOT NULL,
            lv50_atk INT NOT NULL,
            lv50_def INT NOT NULL,
            lv50_int INT NOT NULL,
            lv50_spd INT NOT NULL,
            PRIMARY KEY (number)
        ) USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
    
    def _generate_insert_sql(self, df: pl.DataFrame) -> str:
        """Generate INSERT SQL for Unity Catalog."""
        # Create batch insert statement
        values_list = []
        for row in df.iter_rows():
            values = ", ".join([f"'{str(val)}'" if isinstance(val, str) else str(val) for val in row])
            values_list.append(f"({values})")
        
        values_str = ",\n".join(values_list)
        
        return f"""
        INSERT INTO {self.unity_catalog_table} 
        (number, digimon, stage, type, attribute, memory, equip_slots, 
         lv50_hp, lv50_sp, lv50_atk, lv50_def, lv50_int, lv50_spd)
        VALUES 
        {values_str}
        """
    
    def process_digimon_data(self) -> None:
        """Main processing pipeline: CSV -> SQLModel -> Unity Catalog."""
        print("Starting Digimon data processing pipeline...")
        
        # Step 1: Read CSV data
        df = self.read_csv_data()
        
        # Step 2: Convert to SQLModel records
        records = self.convert_to_sqlmodel_records(df)
        
        # Step 3: Write to Unity Catalog
        self.write_to_unity_catalog(records)
        
        print("Digimon data processing pipeline completed successfully!")


def main():
    """Main function to run the Digimon data processing."""
    # Path to the CSV file (relative to the script location)
    csv_path = Path(__file__).parent.parent.parent.parent.parent / "fixtures" / "DigiDB_digimonlist.csv"
    
    # Unity Catalog table name
    unity_catalog_table = "main.default.digimon_catalog"
    
    # Create processor and run pipeline
    processor = DigimonDataProcessor(str(csv_path), unity_catalog_table)
    processor.process_digimon_data()


if __name__ == "__main__":
    main()
