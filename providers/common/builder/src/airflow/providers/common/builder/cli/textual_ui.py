# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Textual TUI for interactive provider creation."""

from __future__ import annotations

from textual import on
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Input, Label, SelectionList, Static

from airflow.providers.common.builder.config import (
    FEATURE_QUESTIONS,
    NewProviderRequest,
    ProviderFeatures,
)


class ProviderBasicsScreen(Screen):
    """Screen for collecting the --package-name and description."""

    BINDINGS = [
        ("escape", "cancel", "Cancel"),
        ("ctrl+n", "next_screen", "Next"),
    ]

    def __init__(self, description: str = "") -> None:
        super().__init__()
        self._description = description

    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Static("[bold cyan]Airflow Provider Builder[/bold cyan]", id="title"),
            Static("[dim]Step 1 of 2: Package Name[/dim]", id="subtitle"),
            Vertical(
                Label("Package Name (airflow.providers.<path>.<ClassName>):"),
                Input(
                    placeholder="airflow.providers.my_company.MyProvider",
                    id="package_name",
                ),
                Static("", id="package_name_error", classes="error-message"),
                Label("Description:"),
                Input(
                    placeholder="An Apache Airflow provider package.",
                    value=self._description,
                    id="description",
                ),
                Horizontal(
                    Button("Next (Ctrl+N)", variant="primary", id="next_button"),
                    Button("Cancel (Esc)", variant="default", id="cancel_button"),
                    classes="button-row",
                ),
                classes="form-container",
            ),
        )
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#package_name", Input).focus()

    def _validate(self) -> bool:
        pkg_input = self.query_one("#package_name", Input)
        error_widget = self.query_one("#package_name_error", Static)

        is_valid, error_msg = NewProviderRequest.validate_package_name(pkg_input.value.strip())
        if not is_valid:
            error_widget.update(f"[red]{error_msg}[/red]")
            return False
        error_widget.update("")
        return True

    def action_next_screen(self) -> None:
        self._handle_next()

    @on(Button.Pressed, "#next_button")
    def _handle_next(self) -> None:
        if self._validate():
            package_name = self.query_one("#package_name", Input).value.strip()
            description = (
                self.query_one("#description", Input).value.strip() or "An Apache Airflow provider package."
            )
            self.app.push_screen(ComponentSelectionScreen(package_name=package_name, description=description))

    def action_cancel(self) -> None:
        self.app.exit()

    @on(Button.Pressed, "#cancel_button")
    def _handle_cancel(self) -> None:
        self.action_cancel()


class ComponentSelectionScreen(Screen):
    """Screen for selecting provider features."""

    BINDINGS = [
        ("escape", "go_back", "Back"),
        ("ctrl+a", "toggle_all", "Toggle All"),
        ("ctrl+n", "create_provider", "Create"),
    ]

    def __init__(self, package_name: str, description: str) -> None:
        super().__init__()
        self._package_name = package_name
        self._description = description

    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Static("[bold cyan]Airflow Provider Builder[/bold cyan]", id="title"),
            Static("[dim]Step 2 of 2: Select Features[/dim]", id="subtitle"),
            Horizontal(
                SelectionList[str](
                    *[(question, name, True) for name, question in FEATURE_QUESTIONS],
                    id="component_selection",
                ),
                Vertical(
                    Static("[bold]Package Information[/bold]", id="info-title"),
                    Static(f"[cyan]Package:[/cyan] {self._package_name}", id="info-content"),
                    Static("", id="selection-count"),
                    Horizontal(
                        Button("Create (Ctrl+N)", variant="success", id="create_button"),
                        Button("Back (Esc)", variant="default", id="back_button"),
                        classes="button-row",
                    ),
                    id="info-panel",
                ),
                id="main-content",
            ),
        )
        yield Footer()

    def on_mount(self) -> None:
        self._update_count()

    @on(SelectionList.SelectedChanged)
    def _update_count(self) -> None:
        sl = self.query_one(SelectionList)
        count = len(sl.selected)
        total = len(FEATURE_QUESTIONS)
        self.query_one("#selection-count", Static).update(
            f"[bold green]{count}[/bold green] of [bold]{total}[/bold] features selected"
        )

    def action_toggle_all(self) -> None:
        sl = self.query_one(SelectionList)
        if len(sl.selected) < len(FEATURE_QUESTIONS):
            sl.select_all()
        else:
            sl.deselect_all()

    def action_create_provider(self) -> None:
        self.app.exit(result=self._build_request())

    @on(Button.Pressed, "#create_button")
    def _handle_create(self) -> None:
        self.action_create_provider()

    def action_go_back(self) -> None:
        self.app.pop_screen()

    @on(Button.Pressed, "#back_button")
    def _handle_back(self) -> None:
        self.action_go_back()

    def _build_request(self) -> NewProviderRequest:
        sl = self.query_one(SelectionList)
        selected = set(sl.selected)
        kwargs = {name: name in selected for name, _ in FEATURE_QUESTIONS}
        features = ProviderFeatures(**kwargs)
        return NewProviderRequest.from_package_name(
            self._package_name,
            description=self._description,
            features=features,
        )


class ProviderBuilderApp(App):
    """Textual app for building Airflow providers."""

    CSS = """
    Screen { align: center middle; }
    #title { text-align: center; margin: 1; padding: 1; }
    #subtitle { text-align: center; margin-bottom: 1; }
    #main-content { width: 100%; height: auto; margin: 1; }
    #component_selection { width: 3fr; height: 100%; min-height: 30; }
    #info-panel { width: 2fr; height: 100%; padding: 1 2; }
    #info-title { text-align: center; margin-bottom: 1; }
    #info-content { padding: 1; margin-bottom: 2; }
    #selection-count { text-align: center; margin: 2 0; padding: 1; }
    .form-container { width: 80; padding: 1 2; }
    .form-container Label { margin-top: 1; }
    .form-container Input { margin-bottom: 1; }
    .error-message { min-height: 1; margin-bottom: 1; }
    .button-row { margin-top: 2; width: 100%; align: center middle; }
    .button-row Button { margin: 0 1; }
    Container { width: 100%; height: 100%; }
    """

    def __init__(self, description: str = "") -> None:
        super().__init__()
        self._description = description
        self.result: NewProviderRequest | None = None

    def on_mount(self) -> None:
        self.push_screen(ProviderBasicsScreen(description=self._description))

    def exit(self, result=None, return_code: int = 0) -> None:
        if result is not None:
            self.result = result
        super().exit(return_code=return_code)


def run_textual_ui(
    output_dir: str = ".",
    description: str = "",
) -> NewProviderRequest | None:
    """Run the Textual UI and return a ``NewProviderRequest`` or ``None`` on cancel."""
    app = ProviderBuilderApp(description=description)
    app.run()
    if app.result is not None:
        app.result.output_dir = output_dir
    return app.result
