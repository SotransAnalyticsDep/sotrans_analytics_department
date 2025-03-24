"""
Модуль отвечает за предоставление VBA-кода Excel.
"""

# VBA-код, отвечающий за разъединение всех объединённых ячеек на всех листах рабочей книги
VBA_UNMERGE_ALL_CELLS: str = '''
Sub unmerge_all_cells()
    Dim ws As Worksheets
    For Each ws In ThisWorkbook.Worksheets
        ws.Cells.UnMerge
    Next ws
End Sub
'''
