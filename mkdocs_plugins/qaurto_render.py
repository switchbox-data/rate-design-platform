import subprocess
import threading
import time
from pathlib import Path

from mkdocs.config import config_options
from mkdocs.plugins import BasePlugin
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class QmdChangeHandler(FileSystemEventHandler):
    def __init__(self, quarto_cmd, output_format):
        self.quarto_cmd = quarto_cmd
        self.output_format = output_format

    def on_modified(self, event):
        if event.is_directory or not str(event.src_path).endswith(".qmd"):
            return

        qmd_path = Path(str(event.src_path))
        md_path = qmd_path.with_suffix(".md")

        # Render only if .md is missing or older than .qmd
        if not md_path.exists() or qmd_path.stat().st_mtime > md_path.stat().st_mtime:
            print(f"[quarto-render] Detected change in {qmd_path}, rendering...")
            try:
                # Trusted input: quarto_cmd and output_format come from config
                subprocess.run(  # noqa: S603
                    [self.quarto_cmd, "render", str(qmd_path), "--to", self.output_format],
                    check=True,
                )
                print(f"[quarto-render] Rendered {qmd_path}")
            except subprocess.CalledProcessError:
                print(f"[quarto-render] Failed to render {qmd_path}")
        else:
            print(f"[quarto-render] No rendering needed for {qmd_path} (already up-to-date)")


class QuartoRenderPlugin(BasePlugin):
    config_scheme = (
        ("quarto_cmd", config_options.Type(str, default="quarto")),
        ("source_dir", config_options.Type(str, default="docs")),
        ("output_format", config_options.Type(str, default="gfm")),
    )

    def on_pre_build(self, config):
        source_dir = Path(self.config["source_dir"])
        quarto = self.config["quarto_cmd"]
        fmt = self.config["output_format"]

        print(f"[quarto-render] Initial rendering of .qmd files in {source_dir}")

        for qmd_path in source_dir.rglob("*.qmd"):
            md_path = qmd_path.with_suffix(".md")

            # Only render if .md does not exist or is older than .qmd
            if not md_path.exists() or qmd_path.stat().st_mtime > md_path.stat().st_mtime:
                try:
                    # Trusted input: quarto and fmt come from config
                    subprocess.run(  # noqa: S603
                        [quarto, "render", str(qmd_path), "--to", fmt],
                        check=True,
                    )
                    print(f"[quarto-render] Rendered: {qmd_path}")
                except subprocess.CalledProcessError:
                    print(f"[quarto-render] Failed: {qmd_path}")
                    raise
            else:
                print(f"[quarto-render] Skipping (up-to-date): {qmd_path}")

    def on_serve(self, server, config, builder):
        # Set up a watchdog observer to monitor .qmd file changes during `mkdocs serve`
        source_dir = self.config["source_dir"]
        quarto = self.config["quarto_cmd"]
        fmt = self.config["output_format"]

        event_handler = QmdChangeHandler(quarto, fmt)
        observer = Observer()
        observer.schedule(event_handler, path=source_dir, recursive=True)
        observer.start()

        print(f"[quarto-render] Watching for .qmd changes in {source_dir}...")

        def _watchdog_loop():
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                observer.stop()
            observer.join()

        # Run watchdog loop in a separate thread so MkDocs can continue serving
        thread = threading.Thread(target=_watchdog_loop, daemon=True)
        thread.start()

        return server
