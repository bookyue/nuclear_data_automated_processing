import openpyxl as xl


def get_column_index(filename):
    """
    :param filename: input file
    :return: data rang of column ex: (2, 32)
    """
    start_column_index = 2
    wb = xl.load_workbook(filename)
    sheet = wb.worksheets[0]
    # print(sheet)
    max_column = sheet.max_column
    # print(max_column)
    return start_column_index, max_column
