import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog
from queue import Queue
import threading as mt

from loguru import logger

from .log_handler import LogHandler
from .functions import main


class ArticleCrossingApp(tk.Tk):
    # ИНИЦИАЛИЗАЦИЯ;
    def __init__(self) -> None:
        super().__init__()
        self.title("Article Crossing Tool")
        self.geometry("1280x720")

        self.file_path = tk.StringVar()
        self.log_queue = Queue()
        self.input_queue = Queue()
        self.response_queue = Queue()

        self.configure_logger()
        self.create_widgets()
        self.after(100, self.process_queues)

    def configure_logger(self) -> None:
        logger.remove()
        logger.add(
            LogHandler(log_queue=self.log_queue),
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
            level="DEBUG",
        )

    def create_widgets(self) -> None:
        # Выбор файла;
        file_frame = ttk.Frame(self)
        file_frame.pack(pady=10)

        ttk.Label(file_frame, text="Excel-файл:").pack(side=tk.LEFT)
        ttk.Entry(file_frame, textvariable=self.file_path, width=70).pack(
            side=tk.LEFT, padx=5
        )
        ttk.Button(file_frame, text="Открыть", command=self.browse_file).pack(
            side=tk.LEFT
        )

        # Область логирования;
        log_frame = ttk.LabelFrame(self, text="Логи")
        log_frame.pack(expand=True, fill=tk.BOTH, padx=10, pady=5)

        self.log_area = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD)
        self.log_area.pack(expand=True, fill=tk.BOTH)

        # Контроль;
        self.run_button = ttk.Button(
            self, text="Запуск кроссировки", command=self.start_processing
        )
        self.run_button.pack(pady=10)

        # Статус бар;
        self.status = ttk.Label(self, relief=tk.SUNKEN)
        self.status.pack(side=tk.BOTTOM, fill=tk.X)

    def browse_file(self) -> None:
        path: str = filedialog.askopenfilename(filetypes=[('Excel файлы', '*.xlsx')])
        if path:
            self.file_path.set(path)
            logger.info(f'Файл выбран: {path}')

    def start_processing(self) -> None:
        path: str = self.file_path.get()
        if not path:
            logger.error('Excel-файл не выбран')
            tk.messagebox.showerror('Error', 'Сначала необходимо выберать Excel-файл')
            return
        
        self.run_button.config(state=tk.DISABLED)
        self.status.config(text='Процесс...')

        processing_thread: mt.Thread = mt.Thread(
            target=self.run_processing_pipeline,
            args=(path, ),
            daemon=True
        )

        processing_thread.start()

    def run_processing_pipeline(self, path: str) -> None:
        try:
            logger.info('Запуск пайплайна')
            main(path, self.input_callback)
            logger.success('Пайплайн успешно завершен')
        except Exception as e:
            logger.exception(f'Исключение в процессе пайплайна: {e}')
        finally:
            self.after(0, self.reset_ui)

    def input_callback(self):
        self.input_queue.put(True)
        return self.response_queue.get()
    
    def process_queues(self):
        # Process log messages
        while not self.log_queue.empty():
            msg = self.log_queue.get()
            self.log_area.insert(tk.END, msg)
            self.log_area.see(tk.END)
        
        # Process input requests
        if not self.input_queue.empty():
            self.input_queue.get()
            response = tk.messagebox.askyesno("Подтверждение", "Продолжить выполнение?")
            self.response_queue.put(response)
        
        self.after(100, self.process_queues)

    def reset_ui(self):
        self.run_button.config(state=tk.NORMAL)
        self.status.config(text="Готово")
