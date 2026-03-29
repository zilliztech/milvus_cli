try:
    from .Types import ParameterException
except ImportError:
    from Types import ParameterException
import os


def readCsvFile(path="", withCol=True):
    import re

    pattern = re.compile(".+\..+\/.*")
    isUrl = pattern.match(path)
    if isUrl:
        return readCsvFileFromUrl(path, withCol)
    return readCsvFileFromLocal(path, withCol)


def readCsvFileFromLocal(path="", withCol=True):
    from csv import reader
    from json import JSONDecodeError
    import click

    click.echo("Reading file from local path.")
    if not path or not path[-4:] == ".csv":
        raise ParameterException("Path is empty or target file is not .csv")
    fileSize = os.stat(path).st_size
    if fileSize >= 512000000:
        raise ParameterException(
            "File is too large! Only allow csv files less than 512MB."
        )
    try:
        result = {"columns": [], "data": []}
        with click.open_file(path, "r") as csv_file:
            click.echo(f"Opening csv file({fileSize} bytes)...")
            csv_reader = reader(csv_file, delimiter=",")
            handleCsvFile(result, csv_reader, withCol)
    except FileNotFoundError as fe:
        raise ParameterException(f"FileNotFoundError {str(fe)}")
    except UnicodeDecodeError as ue:
        raise ParameterException(f"UnicodeDecodeError {str(ue)}")
    except JSONDecodeError as je:
        raise ParameterException(f"JSONDecodeError {str(je)}")
    else:
        return result


def readCsvFileFromUrl(url="", withCol=True):
    import requests
    from csv import reader
    from json import JSONDecodeError
    import click

    click.echo("Reading file from remote URL.")
    try:
        result = {"columns": [], "data": []}
        with requests.Session() as s:
            download = s.get(url)
            decoded_content = download.content.decode("utf-8")
            csv_reader = reader(decoded_content.splitlines(), delimiter=",")
            handleCsvFile(result, csv_reader, withCol)
    except FileNotFoundError as fe:
        raise ParameterException(f"FileNotFoundError {str(fe)}")
    except UnicodeDecodeError as ue:
        raise ParameterException(f"UnicodeDecodeError {str(ue)}")
    except JSONDecodeError as je:
        raise ParameterException(f"JSONDecodeError {str(je)}")
    else:
        return result


def handleCsvFile(result, csv_reader, withCol):
    import click

    # For progressbar, transform it to list.
    rows = list(csv_reader)
    line_count = 0
    with click.progressbar(rows, label="Reading csv rows...", show_percent=True) as bar:
        # for row in csv_reader:
        for row in bar:
            if withCol and line_count == 0:
                result["columns"] = row
                line_count += 1
            else:
                formatRowForData(row, result["data"])
                line_count += 1
    click.echo(f"""Column names are {result['columns']}""")
    click.echo(f"Processed {line_count} lines.")


# For readCsvFile formatting data.
def formatRowForData(row=[], data=[]):
    from json import loads

    # init data with empty list
    if not data:
        for _in in range(len(row)):
            data.append([])
    for idx, val in enumerate(row):
        formattedVal = loads(val)
        data[idx].append(formattedVal)


def writeCsvFile(path, rows, headers=[]):
    if not path:
        raise ParameterException(f"Path should not be empty")
    from csv import writer
    import click

    try:
        with click.open_file(path, "w+") as csv_file:
            csv_writer = writer(csv_file, delimiter=",")
            if headers:
                csv_writer.writerow(headers)
            line_count = 0
            with click.progressbar(
                rows, label="Writing csv rows...", show_percent=True
            ) as bar:
                for row in bar:
                    csv_writer.writerow(row)
                    line_count += 1
            click.echo(f"Processed {line_count} lines.")
    except Exception as e:
        raise ParameterException(f"Export csv file error! {str(e)}")
