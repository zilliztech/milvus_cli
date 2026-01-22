import json
import csv
import io
from tabulate import tabulate


class OutputFormatter:
    """Global output formatter supporting table, json, and csv formats."""

    FORMATS = ["table", "json", "csv"]
    DEFAULT_TABLEFMT = "grid"

    def __init__(self):
        self._format = "table"

    @property
    def format(self):
        return self._format

    @format.setter
    def format(self, value):
        if value not in self.FORMATS:
            raise ValueError(f"Invalid format '{value}'. Must be one of: {', '.join(self.FORMATS)}")
        self._format = value

    def format_output(self, data, headers=None, tablefmt="grid"):
        """
        Format data according to current format setting.

        Args:
            data: List of dicts, list of lists, or single dict
            headers: Optional list of header names (for list of lists)
            tablefmt: Table format for tabulate (default: grid)

        Returns:
            Formatted string
        """
        if not data:
            return "No data to display."

        # Type validation
        if not isinstance(data, (dict, list)):
            raise ValueError(f"Invalid data type '{type(data).__name__}'. Must be dict, list of dicts, or list of lists.")

        # Normalize data to list of dicts
        if isinstance(data, dict):
            data = [data]

        # Validate list contents
        if data and not isinstance(data[0], (dict, list, tuple)):
            raise ValueError(f"Invalid data type. List elements must be dicts or lists, got '{type(data[0]).__name__}'.")

        if self._format == "json":
            return self._format_json(data)
        elif self._format == "csv":
            return self._format_csv(data, headers)
        else:  # table
            return self._format_table(data, headers, tablefmt)

    def _format_json(self, data):
        """Format data as JSON."""
        return json.dumps(data, indent=2, default=str, ensure_ascii=False)

    def _format_csv(self, data, headers=None):
        """Format data as CSV."""
        if not data:
            return ""

        output = io.StringIO()

        if isinstance(data[0], dict):
            headers = headers or list(data[0].keys())
            writer = csv.DictWriter(output, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
        else:
            writer = csv.writer(output)
            if headers:
                writer.writerow(headers)
            writer.writerows(data)

        return output.getvalue().strip()

    def _format_table(self, data, headers=None, tablefmt="grid"):
        """Format data as table using tabulate."""
        if not data:
            return "No data to display."

        if isinstance(data[0], dict):
            headers = headers or list(data[0].keys())
            # Use headers to get values in consistent order, handling missing keys
            rows = [[row.get(h, "") for h in headers] for row in data]
        else:
            rows = data

        return tabulate(rows, headers=headers, tablefmt=tablefmt)

    def format_list(self, items, header="Item"):
        """Format a simple list of items."""
        if not items:
            return "No items to display."

        if self._format == "json":
            return json.dumps(items, indent=2, ensure_ascii=False)
        elif self._format == "csv":
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow([header])
            for item in items:
                writer.writerow([item])
            return output.getvalue().strip()
        else:
            data = [[item] for item in items]
            return tabulate(data, headers=[header], tablefmt=self.DEFAULT_TABLEFMT)

    def format_key_value(self, data, key_header="Property", value_header="Value"):
        """Format key-value pairs (for show commands)."""
        if not data:
            return "No data to display."

        if isinstance(data, dict):
            items = list(data.items())
        else:
            items = data  # Assume list of [key, value] pairs

        if self._format == "json":
            if isinstance(data, dict):
                return json.dumps(data, indent=2, default=str, ensure_ascii=False)
            return json.dumps(dict(items), indent=2, default=str, ensure_ascii=False)
        elif self._format == "csv":
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow([key_header, value_header])
            writer.writerows(items)
            return output.getvalue().strip()
        else:
            return tabulate(items, headers=[key_header, value_header], tablefmt="grid")
