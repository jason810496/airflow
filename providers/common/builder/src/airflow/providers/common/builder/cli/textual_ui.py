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

from typing import TYPE_CHECKING

from textual import on
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Input, Label, SelectionList, Static

from airflow.providers.common.builder.cli.config import feature_field_questions
from airflow.providers.common.builder.cli.prompt import to_pascal_case

if TYPE_CHECKING:
    from airflow.providers.common.builder.cli.commands import ProviderBuilderArgs, ProviderContext


class ProviderBasicsScreen(Screen):
    """Screen for collecting basic provider information."""

    BINDINGS = [
        ("escape", "cancel", "Cancel"),
        ("ctrl+n", "next_screen", "Next"),
    ]

    def __init__(
        self,
        provider_name: str | None = None,
        package_name: str | None = None,
        provider_description: str | None = None,
        exclude_features: set[str] | None = None,
    ) -> None:
        super().__init__()
        self.provider_name = provider_name
        self.package_name = package_name
        self.provider_description = provider_description
        self.exclude_features = exclude_features or set()

    def compose(self) -> ComposeResult:
        """Create child widgets for the screen."""
        yield Header()
        yield Container(
            Static("[bold cyan]Airflow Provider Builder[/bold cyan]", id="title"),
            Static("[dim]Step 1 of 2: Basic Information[/dim]", id="subtitle"),
            Vertical(
                Label("Provider Name (snake_case):"),
                Input(
                    placeholder="my_provider",
                    value=self.provider_name or "my_provider",
                    id="provider_name",
                ),
                Static("", id="provider_name_error", classes="error-message"),
                Label("Package Name:"),
                Input(
                    placeholder="apache-airflow-providers-my-provider",
                    value=self.package_name or "",
                    id="package_name",
                ),
                Static("", id="package_name_error", classes="error-message"),
                Label("Provider Description:"),
                Input(
                    placeholder="MyProvider provider",
                    value=self.provider_description or "",
                    id="description",
                ),
                Static("", id="description_error", classes="error-message"),
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
        """Handle mount event."""
        provider_input = self.query_one("#provider_name", Input)
        provider_input.focus()
        self.update_package_name()

    @on(Input.Changed, "#provider_name")
    def update_package_name(self) -> None:
        """Auto-update package name based on provider name."""
        provider_input = self.query_one("#provider_name", Input)
        package_input = self.query_one("#package_name", Input)
        description_input = self.query_one("#description", Input)

        provider_name = provider_input.value.strip()

        # Auto-fill package name if empty
        if not package_input.value or package_input.value.startswith("apache-airflow-providers-"):
            package_input.value = f"apache-airflow-providers-{provider_name.replace('_', '-')}"

        # Auto-fill description if empty
        if not description_input.value:
            description_input.value = f"{to_pascal_case(provider_name)} provider"

    def validate_inputs(self) -> bool:
        """Validate all inputs."""
        from airflow.providers.common.builder.cli.commands import ProviderBuilderArgs

        provider_input = self.query_one("#provider_name", Input)
        package_input = self.query_one("#package_name", Input)
        description_input = self.query_one("#description", Input)

        provider_error = self.query_one("#provider_name_error", Static)
        package_error = self.query_one("#package_name_error", Static)
        description_error = self.query_one("#description_error", Static)

        valid = True

        # Validate provider name
        is_valid, error_msg = ProviderBuilderArgs.validate_provider_name(provider_input.value)
        if not is_valid:
            provider_error.update(f"[red]{error_msg}[/red]")
            valid = False
        else:
            provider_error.update("")

        # Validate package name
        is_valid, error_msg = ProviderBuilderArgs.validate_package_name(package_input.value)
        if not is_valid:
            package_error.update(f"[red]{error_msg}[/red]")
            valid = False
        else:
            package_error.update("")

        # Validate description
        is_valid, error_msg = ProviderBuilderArgs.validate_description(description_input.value)
        if not is_valid:
            description_error.update(f"[red]{error_msg}[/red]")
            valid = False
        else:
            description_error.update("")

        return valid

    def action_next_screen(self) -> None:
        """Move to component selection screen via keyboard shortcut."""
        self.handle_next()

    @on(Button.Pressed, "#next_button")
    def handle_next(self) -> None:
        """Move to component selection screen."""
        if self.validate_inputs():
            provider_input = self.query_one("#provider_name", Input)
            package_input = self.query_one("#package_name", Input)
            description_input = self.query_one("#description", Input)

            provider_name = provider_input.value.strip()
            package_name = package_input.value.strip()
            provider_description = description_input.value.strip()

            self.app.push_screen(
                ComponentSelectionScreen(
                    provider_name=provider_name,
                    package_name=package_name,
                    provider_description=provider_description,
                    exclude_features=self.exclude_features,
                )
            )

    def action_cancel(self) -> None:
        """Cancel and exit."""
        self.app.exit()

    @on(Button.Pressed, "#cancel_button")
    def handle_cancel(self) -> None:
        """Cancel button handler."""
        self.action_cancel()


class ComponentSelectionScreen(Screen):
    """Screen for selecting provider components."""

    BINDINGS = [
        ("escape", "go_back", "Back"),
        ("ctrl+a", "toggle_all", "Toggle All"),
        ("ctrl+n", "create_provider", "Create"),
    ]

    def __init__(
        self,
        provider_name: str,
        package_name: str,
        provider_description: str,
        exclude_features: set[str] | None = None,
    ) -> None:
        super().__init__()
        self.provider_name = provider_name
        self.package_name = package_name
        self.provider_description = provider_description
        self.exclude_features = exclude_features or set()

    def compose(self) -> ComposeResult:
        """Create child widgets for the screen."""
        yield Header()
        yield Container(
            Static("[bold cyan]Airflow Provider Builder[/bold cyan]", id="title"),
            Static("[dim]Step 2 of 2: Select Components[/dim]", id="subtitle"),
            Horizontal(
                # Left panel - Feature selection
                SelectionList[str](
                    *self._create_selections(),
                    id="component_selection",
                ),
                # Right panel - Provider info and actions
                Vertical(
                    Static("[bold]Provider Information[/bold]", id="info-title"),
                    Static(
                        f"[cyan]Provider Name:[/cyan]\n{self.provider_name}\n\n"
                        f"[cyan]Package Name:[/cyan]\n{self.package_name}\n\n"
                        f"[cyan]Description:[/cyan]\n{self.provider_description}\n\n"
                        f"[cyan]Class Name:[/cyan]\n{to_pascal_case(self.provider_name)}",
                        id="info-content",
                    ),
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
        """Handle mount event."""
        selection_list = self.query_one(SelectionList)
        selection_list.border_title = "Components (Space: toggle, Ctrl+A: all)"
        self.update_selection_count()

    @on(SelectionList.SelectedChanged)
    def update_selection_count(self) -> None:
        """Update the selection count display."""
        selection_list = self.query_one(SelectionList)
        count = len(selection_list.selected)
        total = len(feature_field_questions)
        count_widget = self.query_one("#selection-count", Static)
        count_widget.update(f"[bold green]{count}[/bold green] of [bold]{total}[/bold] components selected")

    def _create_selections(self) -> list[tuple[str, str, bool]]:
        """Create selection tuples for each component."""
        selections = []
        for field, question in feature_field_questions:
            # Default to selected unless in exclude list
            selected = field not in self.exclude_features
            selections.append((question, field, selected))
        return selections

    def action_toggle_all(self) -> None:
        """Toggle all selections."""
        selection_list = self.query_one(SelectionList)
        # If any unselected, select all. Otherwise deselect all.
        any_unselected = len(selection_list.selected) < len(feature_field_questions)
        if any_unselected:
            selection_list.select_all()
        else:
            selection_list.deselect_all()

    def action_create_provider(self) -> None:
        """Create the provider via keyboard shortcut."""
        self.app.exit(result=self.get_selections())

    @on(Button.Pressed, "#create_button")
    def handle_create(self) -> None:
        """Create button handler."""
        self.action_create_provider()

    def action_go_back(self) -> None:
        """Go back to previous screen via keyboard shortcut."""
        self.app.pop_screen()

    @on(Button.Pressed, "#back_button")
    def handle_back(self) -> None:
        """Back button handler."""
        self.action_go_back()

    def get_selections(self) -> ProviderContext:
        """Get the selected components as a context dict."""
        selection_list = self.query_one(SelectionList)
        selected_fields = set(selection_list.selected)

        # Build context with all fields set to 'n' by default
        context: ProviderContext = {
            "provider_name": self.provider_name,
            "package_name": self.package_name,
            "provider_description": self.provider_description,
            "name": self.provider_name,
            "package-name": self.package_name,
            "description": self.provider_description,
            "class_name": to_pascal_case(self.provider_name),
            "include_all": "n",
            **{field: "y" if field in selected_fields else "n" for field, _ in feature_field_questions},
        }
        return context


class ProviderBuilderApp(App):
    """Textual app for building Airflow providers."""

    CSS = """
    Screen {
        align: center middle;
    }

    #title {
        text-align: center;
        margin: 1;
        padding: 1;
    }

    #subtitle {
        text-align: center;
        margin-bottom: 1;
    }

    #main-content {
        width: 100%;
        height: auto;
        margin: 1;
    }

    #component_selection {
        width: 3fr;
        height: 100%;
        min-height: 30;
    }

    #component_selection > .selection-list--button-highlighted {
        background: $success 30%;
    }

    #component_selection > .selection-list--button-selected {
        color: $success;
    }

    #info-panel {
        width: 2fr;
        height: 100%;
        padding: 1 2;
        background: $panel;
        border: solid $primary;
    }

    #info-title {
        text-align: center;
        margin-bottom: 1;
        color: $text;
    }

    #info-content {
        padding: 1;
        margin-bottom: 2;
        border: solid $accent;
        background: $surface;
    }

    #selection-count {
        text-align: center;
        margin: 2 0;
        padding: 1;
    }

    .form-container {
        width: 80;
        padding: 1 2;
        background: $panel;
        border: solid $primary;
    }

    .form-container Label {
        margin-top: 1;
        color: $text-muted;
    }

    .form-container Input {
        margin-bottom: 1;
    }

    .error-message {
        min-height: 1;
        margin-bottom: 1;
    }

    .button-row {
        margin-top: 2;
        width: 100%;
        align: center middle;
    }

    .button-row Button {
        margin: 0 1;
    }

    Container {
        width: 100%;
        height: 100%;
    }
    """

    def __init__(
        self,
        provider_name: str | None = None,
        package_name: str | None = None,
        provider_description: str | None = None,
        exclude_features: set[str] | None = None,
    ) -> None:
        super().__init__()
        self.provider_name = provider_name
        self.package_name = package_name
        self.provider_description = provider_description
        self.exclude_features = exclude_features or set()
        self.result: ProviderContext | None = None

    def on_mount(self) -> None:
        """Handle mount event."""
        self.push_screen(
            ProviderBasicsScreen(
                provider_name=self.provider_name,
                package_name=self.package_name,
                provider_description=self.provider_description,
                exclude_features=self.exclude_features,
            )
        )

    def exit(self, result=None, return_code: int = 0) -> None:
        """Override exit to capture result."""
        if result is not None:
            self.result = result
        super().exit(return_code=return_code)


def run_textual_ui(args: ProviderBuilderArgs) -> ProviderContext | None:
    """Run the Textual UI and return the provider context."""
    app = ProviderBuilderApp(
        provider_name=args.provider_name,
        package_name=args.package_name,
        provider_description=args.provider_description,
        exclude_features=args.exclude_features,
    )
    app.run()
    return app.result
