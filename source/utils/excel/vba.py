"""
Модуль отвечает за доступ к VBA-коду.
"""


# ##################################################
# FUNCTIONS
# ##################################################
def get_unmerge_all_cells() -> str:
    """
    Функция предоставляет VBA-код для отмены объединения ячеек на всех листах 
    рабочей книги Excel.
    """
    
    return """
Sub unmerge_all_cells()
    Dim ws As Worksheet
    For Each ws In ThisWorkbook.Worksheets
        ws.Cells.Unmerge
    Next ws
End Sub
"""
