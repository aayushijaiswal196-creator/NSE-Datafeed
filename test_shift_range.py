from sheet_service import shift_column_range, write_to_cell_sheet

SPREADSHEET_ID = "1ZbIM2t5x-g7KyaZFxw635-foX0AkK0ET2R2quUk6G8Y"
SHEET_ID = "Sheet5"

# Setup: Put some data in G3:G6
#print("Setting up data in G3:G6...")
#data = ["Val1", "Val2", "Val3", "Val4"]
#for i, val in enumerate(data):
#    write_to_cell_sheet(SPREADSHEET_ID, SHEET_ID, f"G{3+i}", val)

print("\nExecuting shift from G3:G6 to C3:C6...")
shift_result = shift_column_range(SPREADSHEET_ID, SHEET_ID, "G3:G6", "C3:C6")
print("Shift Result:", shift_result)
