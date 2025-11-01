#!/usr/bin/env python3
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

# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "rich>=13.6.0",
#   "jinja2>=3.1.5",
# ]
# ///

"""
GitHub Copilot CLI-based translation service.

This module provides a simplified interface to GitHub Copilot for translation
by leveraging the gh CLI with the copilot extension via subprocess calls.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import unicodedata
from pathlib import Path
from typing import TYPE_CHECKING

from jinja2 import Environment, FileSystemLoader, TemplateNotFound
from rich import print
from rich.console import Console

if TYPE_CHECKING:
    from jinja2 import Template


# Create a focused prompt for translation with explicit Unicode handling
LANGUAGE_NAMES = {
    "ar": "Arabic (العربية)",
    "ca": "Catalan (Català)",
    "de": "German (Deutsch)",
    "es": "Spanish (Español)",
    "fr": "French (Français)",
    "he": "Hebrew (עברית)",
    "hi": "Hindi (हिन्दी)",
    "hu": "Hungarian (Magyar)",
    "it": "Italian (Italiano)",
    "ja": "Japanese (日本語)",
    "ko": "Korean (한국어)",
    "nl": "Dutch (Nederlands)",
    "pl": "Polish (Polski)",
    "pt": "Portuguese (Português)",
    "tr": "Turkish (Türkçe)",
    "zh-CN": "Simplified Chinese (简体中文)",
    "zh-TW": "Traditional Chinese (繁體中文)",
}
TODO_PREFIX = "TODO: translate:"


class CopilotTranslator:
    """
    A simplified GitHub Copilot CLI-based translator for translation services.

    This class uses the GitHub CLI (gh) with the copilot extension to translate
    JSON translation files. It handles checking for CLI availability and provides
    methods for translating using subprocess calls to the gh copilot command.
    """

    def __init__(self, console: Console | None = None) -> None:
        """Initialize the CopilotTranslator."""
        self.console = console or Console(force_terminal=True, color_system="auto")
        self.max_retries = 3

        # Set up Jinja2 environment for prompt templates
        self.prompts_dir = Path(__file__).parent / "prompts"
        self.jinja_env = Environment(
            loader=FileSystemLoader(
                [
                    str(self.prompts_dir),  # For global.jinja2
                    str(self.prompts_dir / "locales"),  # For language-specific templates
                ]
            ),
            autoescape=False,
        )
        self.template_cache: dict[str, Template] = {}

        # Check for gh CLI and copilot extension availability
        self._check_cli_availability()

    def _check_cli_availability(self) -> None:
        """Check if gh CLI and copilot extension are available.

        Exits with instructions if not available.
        """
        # Check if gh CLI is installed
        if not shutil.which("gh"):
            self.console.print("[red]Error: GitHub CLI (gh) is not installed.[/red]")
            self.console.print("\n[yellow]To install GitHub CLI:[/yellow]")
            self.console.print("  macOS: brew install gh")
            self.console.print("  Linux: https://github.com/cli/cli#installation")
            self.console.print("  Windows: https://github.com/cli/cli#installation")
            sys.exit(1)
        
        # Check if copilot extension is installed
        try:
            result = subprocess.run(
                ["gh", "extension", "list"],
                capture_output=True,
                text=True,
                check=True
            )
            if "github/gh-copilot" not in result.stdout.lower():
                self.console.print("[yellow]GitHub Copilot extension is not installed.[/yellow]")
                self.console.print("\n[cyan]Installing GitHub Copilot extension...[/cyan]")
                subprocess.run(["gh", "extension", "install", "github/gh-copilot"], check=True)
                self.console.print("[green]GitHub Copilot extension installed successfully![/green]")
        except subprocess.CalledProcessError as e:
            self.console.print(f"[red]Error checking gh extensions: {e}[/red]")
            sys.exit(1)
    
    def _get_copilot_token(self) -> str | None:
        """Get Copilot token using gh CLI.

        :return: The Copilot token or None if failed.
        """
        try:
            result = subprocess.run(
                ["gh", "api", "/copilot_internal/v2/token"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode != 0:
                self.console.print(f"[red]Failed to get Copilot token: {result.stderr}[/red]")
                return None
            
            response = json.loads(result.stdout)
            return response.get("token")
            
        except Exception as e:
            self.console.print(f"[red]Error getting Copilot token: {e}[/red]")
            return None
    
    def _copilot_complete(self, prompt: str, retries: int = 0) -> str:
        """Get completion from GitHub Copilot using gh CLI to call the API.

        :param prompt: The prompt to send to Copilot.
        :param retries: The current number of retries attempted.
        :return: The completion text from Copilot.
        """
        if retries > self.max_retries:
            self.console.print("[red]Exceeded maximum retries for Copilot completion.[/red]")
            return ""
        
        if retries > 0:
            self.console.print(
                f"[yellow]Retrying Copilot completion (attempt {retries}/{self.max_retries})...[/yellow]"
            )
        
        try:
            # Get Copilot token
            token = self._get_copilot_token()
            if not token:
                return ""
            
            # Prepare the request payload
            payload = {
                "prompt": prompt,
                "suffix": "",
                "max_tokens": 2000,
                "temperature": 0.1,
                "top_p": 1,
                "n": 1,
                "stop": ["\n"],
                "nwo": "github/copilot.vim",
                "stream": True,
                "extra": {"language": "text"}
            }
            
            # Use gh api to call Copilot completions endpoint
            # We use curl via subprocess since gh api doesn't support streaming well
            # Note: Token is passed as command-line arg. This is acceptable for a dev tool
            # as the token is short-lived and only visible briefly in process lists.
            result = subprocess.run(
                [
                    "curl", "-s",
                    "https://copilot-proxy.githubusercontent.com/v1/engines/copilot-codex/completions",
                    "-H", f"authorization: Bearer {token}",
                    "-H", "content-type: application/json",
                    "-d", json.dumps(payload)
                ],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                self.console.print(f"[red]Copilot API error: {result.stderr}[/red]")
                if retries < self.max_retries:
                    return self._copilot_complete(prompt, retries + 1)
                return ""
            
            # Parse the streaming response
            output = result.stdout.strip()
            translation_parts = []
            
            for line in output.split("\n"):
                if line.startswith("data: {"):
                    try:
                        json_data = json.loads(line[6:])  # Remove "data: " prefix
                        choices = json_data.get("choices", [])
                        if choices:
                            text = choices[0].get("text", "")
                            if text:
                                translation_parts.append(text)
                    except json.JSONDecodeError:
                        continue
            
            return "".join(translation_parts).strip()
            
        except subprocess.TimeoutExpired:
            self.console.print("[red]Copilot CLI timed out.[/red]")
            if retries < self.max_retries:
                return self._copilot_complete(prompt, retries + 1)
            return ""
        except Exception as e:
            self.console.print(f"[red]Error calling Copilot CLI: {e}[/red]")
            if retries < self.max_retries:
                return self._copilot_complete(prompt, retries + 1)
            return ""

    def translate(self, lang_path: Path) -> None:
        """Translate a JSON translation file using GitHub Copilot.

        This method loads a JSON translation file, identifies TODO entries,
        and uses GitHub Copilot to provide translations for them.

        :param lang_path: Path to the JSON translation file to translate.
        """

        if not lang_path.exists():
            self.console.print(f"[red]Error: Translation file {lang_path} does not exist[/red]")
            return

        # Determine target language from path
        # Assuming path structure like .../locales/de/common.json
        parts = lang_path.parts
        if "locales" in parts:
            locale_index = parts.index("locales")
            if locale_index + 1 < len(parts):
                target_language = parts[locale_index + 1]
            else:
                target_language = "unknown"
        else:
            target_language = "unknown"

        self.console.print(f"[cyan]Loading translation file: {lang_path}[/cyan]")

        try:
            with open(lang_path, encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            self.console.print(f"[red]Error loading JSON file: {e}[/red]")
            return

        # Find and translate TODO entries
        translations_made = self._translate_recursive(data, target_language, lang_path.stem)

        if translations_made > 0:
            # Save the updated file
            with open(lang_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.write("\n")  # Ensure newline at end

            self.console.print(
                f"[green]Successfully translated {translations_made} entries in {lang_path}[/green]"
            )
        else:
            self.console.print(f"[yellow]No TODO entries found to translate in {lang_path}[/yellow]")

    def _translate_recursive(self, obj: dict, target_language: str, context: str) -> int:
        """Recursively find and translate TODO entries in a JSON structure.

        :param obj: The JSON dictionary object to process.
        :param target_language: The target language code.
        :param context: Context for translation (file name, section, etc.).
        :return: Number of translations made.
        """
        translations_made = 0

        for key, value in obj.items():
            if isinstance(value, str) and value.strip().startswith(TODO_PREFIX):
                # Extract the original text
                original_text = value.replace(TODO_PREFIX, "").strip()
                if original_text:
                    translation = self._get_translation(original_text, target_language, key, context)
                    if translation:
                        obj[key] = translation
                        translations_made += 1
                        self.console.print(
                            f"[green]✓[/green] {context}.{key}: `{original_text}` → `{translation}`"
                        )
                    else:
                        self.console.print(f"[yellow]⚠[/yellow] Failed to translate: {key}")
            elif isinstance(value, dict):
                translations_made += self._translate_recursive(value, target_language, f"{context}.{key}")

        return translations_made

    def _get_translation(self, text: str, target_language: str, key: str, context: str) -> str | None:
        """Get translation for a specific text using GitHub Copilot CLI.

        :param text: The text to translate.
        :param target_language: The target language code.
        :param key: The JSON key for context.
        :param context: Additional context information.
        :return: The translated text or None if translation failed.
        """
        # Get language name for display
        language_name = LANGUAGE_NAMES.get(target_language, target_language)
        # Load global prompt template
        if global_template := self._load_prompt_template("global"):
            prompt = global_template.render(
                language_name=language_name, text=text, key=key, context=context
            ).strip()
            prompt += "\n\n"
        else:
            self.console.print("[red]Error: global.jinja2 template not found.[/red]")
            return None

        # Append language-specific prompt if available
        if language_specific_prompt := self._load_prompt_template(target_language):
            prompt += language_specific_prompt.render().strip()

        try:
            translation = self._copilot_complete(prompt)

            # Clean up the response and ensure proper Unicode handling
            if translation:
                # Remove quotes and clean up
                translation = translation.strip()
                # Remove surrounding quotes if present
                if translation.startswith('"') and translation.endswith('"'):
                    translation = translation[1:-1]
                elif translation.startswith("'") and translation.endswith("'"):
                    translation = translation[1:-1]

                # Ensure proper Unicode decoding if needed
                # Note: This attempts to decode escaped Unicode sequences but may incorrectly
                # process text that legitimately contains the literal string "\\u"
                try:
                    # Try to decode if it's been double-encoded (e.g., "\\u4e2d\\u6587")
                    if isinstance(translation, str) and "\\u" in translation:
                        # Attempt decoding, but keep original if it fails
                        decoded = translation.encode("utf-8").decode("unicode_escape")
                        # Only use decoded version if it's valid
                        translation = decoded
                except (UnicodeDecodeError, UnicodeEncodeError, AttributeError):
                    pass  # Keep original if decoding fails

                # Basic validation - ensure it's not empty and doesn't look like code
                if translation and not translation.startswith("//") and len(translation.strip()) > 0:
                    # Final Unicode normalization
                    translation = unicodedata.normalize("NFC", translation)
                    return translation.strip()

        except Exception as e:
            self.console.print(f"[red]Translation error for '{text}': {e}[/red]")

        return None

    def _load_prompt_template(self, template_name: str) -> Template | None:
        """Load language-specific or global prompt template.

        :param template_name: The name of the template file (e.g., 'zh-TW', 'de' or 'global').
        :return: The Jinja2 template object or None if template not found.
        """
        if template_name in self.template_cache:
            return self.template_cache[template_name]

        try:
            # Try to load language-specific template (e.g., zh-TW.jinja2) or global.jinja2
            template = self.jinja_env.get_template(f"{template_name}.jinja2")
            self.template_cache[template_name] = template
            return template

        except TemplateNotFound:
            return None
        except Exception as e:
            self.console.print(
                f"[yellow]Warning: Error loading template {template_name}.jinja2: {e}[/yellow]"
            )
            return None


# Example usage if we want to run this script directly
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: uv run dev/i18n/copilot_translations.py <path_to_translation_file>")
        sys.exit(1)

    lang_path = Path(sys.argv[1])
    if not lang_path.exists():
        print(f"Error: File {sys.argv[1]} does not exist.")
        sys.exit(1)

    translator = CopilotTranslator()
    translator.translate(lang_path)
