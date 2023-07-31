from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback

import re
from datetime import datetime
from functools import reduce


def id_query(html_document, html_type):
    # Define the regular expression pattern to match the opening and closing tags of the specified HTML type
    pattern = r'<{}.*?>.*?</{}>'.format(html_type, html_type)
    # Use re.findall to find all occurrences of the pattern in the HTML document
    matched_elements = re.findall(pattern, html_document, re.DOTALL)
    return matched_elements


def get_text(html_element):
    # Define the regular expression pattern to match the content inside the HTML element
    pattern = r'<[^>]*>([^<]*)</[^>]*>'
    # Use re.search to find the first occurrence of the pattern in the HTML element
    match = re.search(pattern, html_element)
    if match:
        # Extract the text inside the HTML element
        text_inside_element = match.group(1).strip()
        return text_inside_element
    else:
        return None


def transform_date(date_str):
    # Parse the input date string into a datetime object using the given format
    date_obj = datetime.strptime(date_str, '%d-%b-%Y %I:%M %p')
    # Convert the datetime object into the desired format
    transformed_date_str = date_obj.strftime('%Y-%m-%d %H:%M:%S')
    return transformed_date_str


def transform_country(input_str):
    transformations = [
        lambda x: x.split('/')[0],  # Remove everything after a slash
        # Remove "Republic of" and "West" from the beginning
        lambda x: re.sub(r'^Republic of |^West ', '', x),
        # Remove "South" from the beginning, except for "South Africa" or "South Korea"
        lambda x: re.sub(r'^(?!South Africa\b|South Korea\b)South ', '', x),
        # Remove anything within parentheses
        lambda x: re.sub(r'\(.*\)', '', x),
        # Replace "Gibralter" with "Gibraltar"
        lambda x: x.replace('Gibralter', 'Gibraltar'),
        lambda x: x.strip()  # Trim whitespace
    ]

    return reduce(lambda x, f: f(x), transformations, input_str)


def get_data_from_row(row_html):
    tds = [get_text(td).strip() for td in id_query(row_html, 'td')]
    (event_date, shape, _, state, country, _) = tds
    new_date = transform_date(event_date)
    new_state = state if country == 'USA' else ''
    new_country = transform_country(country)
    return '{},{},{},{}'.format(new_date, shape, new_country, new_state)


def transform_page(html_page):
    first_table = id_query(html_page, 'table')[0]
    data_rows = id_query(first_table, 'tr')[1:]

    csv_rows = ['event_date,shape,country,state']
    for current_row in data_rows:
        csv_rows.append(get_data_from_row(current_row))

    return '\n'.join(csv_rows)


class PyStreamCallback(StreamCallback):
    def __init__(self):
        pass

    def process(self, inputStream, outputStream):
        html_page = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        flow_response = transform_page(html_page)
        outputStream.write(bytearray(flow_response.encode('utf-8')))


flowFile = session.get()

if flowFile is not None:
    try:
        # Get the input data and execute the PyStreamCallback
        flowFile = session.write(flowFile, PyStreamCallback())
        session.transfer(flowFile, REL_SUCCESS)
    except Exception as e:
        # In case of an error, route to "failure" relationship
        log.error("Error processing the flow file: {}".format(e))
        session.transfer(flowFile, REL_FAILURE)
