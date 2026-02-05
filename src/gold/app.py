import logging
import threading
import tkinter as tk
from tkinter import messagebox, ttk

from src.common.logging_config import gold_setup_logging
from src.gold.orchestrator import run_gold_scoring_engine


# -------------------------------
# Logging handler for Tkinter UI
# -------------------------------
class TkinterLogHandler(logging.Handler):
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)

        def append():
            self.text_widget.insert(tk.END, msg + "\n")
            self.text_widget.see(tk.END)

        self.text_widget.after(0, append)


# -------------------------------
# Main Application
# -------------------------------
class GoldScoringApp(tk.Tk):
    def __init__(self):
        super().__init__()

        gold_setup_logging()

        self.title("Gold Scoring Engine")
        self.geometry("800x500")
        self.resizable(False, False)

        self._build_ui()
        self._setup_logging()

    def _build_ui(self):
        # Top control frame
        control_frame = ttk.Frame(self, padding=10)
        control_frame.pack(fill="x")

        ttk.Label(control_frame, text="Macro Phase:").pack(side="left", padx=(0, 5))

        self.macro_phase = tk.StringVar(value="Recovery")
        phase_combo = ttk.Combobox(
            control_frame,
            textvariable=self.macro_phase,
            values=["Recovery", "Expansion", "Peak", "Recession"],
            state="readonly",
            width=15,
        )
        phase_combo.pack(side="left", padx=(0, 20))

        self.run_button = ttk.Button(
            control_frame, text="Run Gold Scoring Engine", command=self.run_engine
        )
        self.run_button.pack(side="left")

        self.done_button = ttk.Button(
            control_frame,
            text="Done",
            command=self.on_done,
            state="disabled",
        )
        self.done_button.pack(side="left", padx=(10, 0))

        # Progress Frame
        progress_frame = ttk.Frame(self, padding=10)
        progress_frame.pack(fill="x")

        self.progress_label = ttk.Label(progress_frame, text="Idle")
        self.progress_label.pack(anchor="w")

        self.progress = ttk.Progressbar(
            progress_frame,
            orient="horizontal",
            length=760,
            mode="determinate",
            maximum=100,
        )
        self.progress.pack(pady=5)

        # Log output
        log_frame = ttk.LabelFrame(self, text="Execution Log", padding=10)
        log_frame.pack(fill="both", expand=True, padx=10, pady=10)

        self.log_text = tk.Text(
            log_frame,
            height=20,
            wrap="word",
            state="normal",
            background="#0f172a",
            foreground="#e5e7eb",
            insertbackground="white",
        )
        self.log_text.pack(fill="both", expand=True)

    def _setup_logging(self):
        handler = TkinterLogHandler(self.log_text)
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        handler.setFormatter(formatter)

        # logging.getLogger().handlers.clear()
        logging.getLogger().addHandler(handler)
        logging.getLogger().setLevel(logging.INFO)

    def run_engine(self):
        macro_phase = self.macro_phase.get()

        # Reset progress UI (UI thread)
        self.progress["value"] = 0
        self.progress_label.config(text="Initializing...")

        self.run_button.config(state="disabled")
        logging.info(f"Triggered Gold Scoring Engine | Macro Phase = {macro_phase}")

        thread = threading.Thread(
            target=self._run_engine_thread, args=(macro_phase,), daemon=True
        )
        thread.start()

        self.done_button.config(state="disabled")

    def _run_engine_thread(self, macro_phase):
        try:
            self.update_progress(0, "Starting execution")

            run_gold_scoring_engine(
                macro_phase,
                progress_callback=self.update_progress,
            )

            messagebox.showinfo("Success", "Gold Scoring Engine completed successfully")
        except Exception as e:
            logging.exception("Execution failed")
            messagebox.showerror("Error", str(e))
        finally:
            self.run_button.config(state="normal")
            self.done_button.config(state="normal")
            self.update_progress(100, "Completed")

    def update_progress(self, value, message):
        def _update():
            self.progress["value"] = value
            self.progress_label.config(text=f"{value}% — {message}")

        self.after(0, _update)

    def on_done(self):
        self.destroy()  # closes the app


# -------------------------------
# Entry Point
# -------------------------------
if __name__ == "__main__":
    app = GoldScoringApp()
    app.mainloop()
