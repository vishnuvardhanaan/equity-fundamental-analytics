import threading
import tkinter as tk
from tkinter import ttk

from src.common.logging_config import silver_setup_logging
from src.silver.orchestrator import run_silver_pipeline

STATUS_ICONS = {
    "PENDING": "⏳",
    "RUNNING": "▶",
    "SUCCESS": "✓",
    "FAILED": "✗",
}


class SilverCleaningApp(tk.Tk):
    def __init__(self):
        super().__init__()

        silver_setup_logging()

        self.title("Equity Data Cleaning Pipeline")
        self.geometry("520x360")
        self.resizable(False, False)

        self.steps_ui = {}

        self._build_ui()

    def _build_ui(self):
        ttk.Label(
            self,
            text="National Stock Exchange Equity Clean Pipeline",
            font=("Segoe UI", 13, "bold"),
        ).pack(pady=12)

        self.steps_frame = ttk.Frame(self)
        self.steps_frame.pack(fill="x", padx=30)

        for step in [
            "Equity Universe",
            "Equity Static Information",
            "Equity Dynamic Information",
            "Balance Sheet",
            "Income Statement",
            "Cashflow Statement",
        ]:
            self._add_step_row(step)

        btn_frame = ttk.Frame(self)
        btn_frame.pack(pady=20)

        self.run_btn = ttk.Button(
            btn_frame,
            text="Clean Raw NSE Equity Database",
            command=self.start_pipeline,
        )
        self.run_btn.pack(side="left")

        self.done_btn = ttk.Button(
            btn_frame,
            text="Done",
            command=self.on_done,
            state="disabled",
        )
        self.done_btn.pack(side="left", padx=(10, 0))

        self.footer = ttk.Label(self, text="Idle")
        self.footer.pack()

    def _add_step_row(self, name):
        row = ttk.Frame(self.steps_frame)
        row.pack(fill="x", pady=4)

        ttk.Label(row, text=name, width=26).pack(side="left")

        status = ttk.Label(row, text=STATUS_ICONS["PENDING"])
        status.pack(side="right")

        self.steps_ui[name] = status

    def update_step_status(self, step_name, status):
        icon = STATUS_ICONS.get(status, "")
        self.steps_ui[step_name].config(text=icon)
        self.footer.config(text=f"{step_name}: {status}")
        self.update_idletasks()

    def start_pipeline(self):
        self.run_btn.config(state="disabled")
        self.done_btn.config(state="disabled")
        self.footer.config(text="Pipeline running...")

        thread = threading.Thread(target=self._run_pipeline, daemon=True)
        thread.start()

    def _run_pipeline(self):
        def callback(step, status):
            self.after(0, self.update_step_status, step, status)

        run_silver_pipeline(status_callback=callback)

        self.after(0, self.pipeline_finished)

    def pipeline_finished(self):
        self.footer.config(
            text="Processed and Cleaned NSE Equity Database. Check logs."
        )
        self.run_btn.config(state="normal")
        self.done_btn.config(state="normal")

    def on_done(self):
        self.destroy()


if __name__ == "__main__":
    app = SilverCleaningApp()
    app.mainloop()
