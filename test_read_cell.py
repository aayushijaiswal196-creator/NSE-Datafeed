from sheet_service import read_from_cell_sheet

SPREADSHEET_ID = "1ZbIM2t5x-g7KyaZFxw635-foX0AkK0ET2R2quUk6G8Y"
SHEET_ID = "Sheet5"
CELL = "A1"

if __name__ == "__main__":
    print(f"Reading from {SPREADSHEET_ID} -> {SHEET_ID}!{CELL}")
    value = read_from_cell_sheet(SPREADSHEET_ID, SHEET_ID, CELL)
    print(f"Value at {CELL}: '{value}'")
