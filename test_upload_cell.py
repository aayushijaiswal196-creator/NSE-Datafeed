from sheet_service import upload_to_cell_sheet

SPREADSHEET_ID = "1ZbIM2t5x-g7KyaZFxw635-foX0AkK0ET2R2quUk6G8Y"
SHEET_ID = "Sheet5"
CELL = "A1"
VALUE = "Ayan"

if __name__ == "__main__":
    print(f"Testing upload to {SPREADSHEET_ID} -> {SHEET_ID}!{CELL} with value '{VALUE}'")
    result = upload_to_cell_sheet(SPREADSHEET_ID, SHEET_ID, CELL, VALUE)
    print("Result:", result)
