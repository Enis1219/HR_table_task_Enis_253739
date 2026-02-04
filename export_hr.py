import mysql.connector
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "hr_user",
    "password": "HrUser_2026!",
    "database": "hr",
}

ROW_COUNT = 10
XML_FILE = "hr_export.xml"
DTD_FILE = "hr_export.dtd"
TABLE_NAME = "employees"

COLUMNS = [
    "EMPLOYEE_ID",
    "FIRST_NAME",
    "LAST_NAME",
    "EMAIL",
    "PHONE_NUMBER",
    "HIRE_DATE",
    "JOB_ID",
    "SALARY",
    "COMMISSION_PCT",
    "MANAGER_ID",
    "DEPARTMENT_ID",
]

TEXT_FIELDS_FOR_CHECKSUM = ["FIRST_NAME", "LAST_NAME", "EMAIL", "PHONE_NUMBER", "JOB_ID"]


def prettify(elem: ET.Element) -> str:
    raw = ET.tostring(elem, encoding="utf-8")
    return minidom.parseString(raw).toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")


def to_text(v) -> str:
    if v is None:
        return ""
    if hasattr(v, "isoformat"):  # dates
        return v.isoformat()
    return str(v)


def checksum(rows) -> int:
    # детерминистично: сума на ключовете + дължините на текстовите полета
    s = 0
    for r in rows:
        s += int(r["EMPLOYEE_ID"])
        for f in TEXT_FIELDS_FOR_CHECKSUM:
            s += len(to_text(r.get(f)))
    return s


def write_dtd() -> None:
    dtd = """<!ELEMENT hrExport (rows, control)>
<!ATTLIST hrExport
  table CDATA #REQUIRED
  exportedAt CDATA #REQUIRED
>

<!ELEMENT rows (row+)>

<!ELEMENT row (EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB_ID, SALARY, COMMISSION_PCT, MANAGER_ID, DEPARTMENT_ID)>
<!ELEMENT EMPLOYEE_ID (#PCDATA)>
<!ELEMENT FIRST_NAME (#PCDATA)>
<!ELEMENT LAST_NAME (#PCDATA)>
<!ELEMENT EMAIL (#PCDATA)>
<!ELEMENT PHONE_NUMBER (#PCDATA)>
<!ELEMENT HIRE_DATE (#PCDATA)>
<!ELEMENT JOB_ID (#PCDATA)>
<!ELEMENT SALARY (#PCDATA)>
<!ELEMENT COMMISSION_PCT (#PCDATA)>
<!ELEMENT MANAGER_ID (#PCDATA)>
<!ELEMENT DEPARTMENT_ID (#PCDATA)>

<!ELEMENT control (rowCount, columnCount, minId, maxId, checksum)>
<!ELEMENT rowCount (#PCDATA)>
<!ELEMENT columnCount (#PCDATA)>
<!ELEMENT minId (#PCDATA)>
<!ELEMENT maxId (#PCDATA)>
<!ELEMENT checksum (#PCDATA)>
"""
    with open(DTD_FILE, "w", encoding="utf-8") as f:
        f.write(dtd)


def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    cur.execute(f"""
        SELECT {", ".join(COLUMNS)}
        FROM {TABLE_NAME}
        ORDER BY EMPLOYEE_ID ASC
        LIMIT %s;
    """, (ROW_COUNT,))

    rows_data = cur.fetchall()
    cur.close()
    conn.close()

    exported_at = datetime.now().isoformat(timespec="seconds")

    min_id = min(int(r["EMPLOYEE_ID"]) for r in rows_data)
    max_id = max(int(r["EMPLOYEE_ID"]) for r in rows_data)

    # Root element hrExport with required attributes
    root = ET.Element("hrExport", attrib={
        "table": TABLE_NAME,
        "exportedAt": exported_at
    })

    # <rows> first
    rows_el = ET.SubElement(root, "rows")
    for r in rows_data:
        row_el = ET.SubElement(rows_el, "row")
        for col in COLUMNS:
            ET.SubElement(row_el, col).text = to_text(r[col])

    # <control> at the end (as required)
    control = ET.SubElement(root, "control")
    ET.SubElement(control, "rowCount").text = str(len(rows_data))
    ET.SubElement(control, "columnCount").text = str(len(COLUMNS))
    ET.SubElement(control, "minId").text = str(min_id)
    ET.SubElement(control, "maxId").text = str(max_id)
    ET.SubElement(control, "checksum").text = str(checksum(rows_data))

    write_dtd()

    xml = prettify(root)
    doctype = f'<!DOCTYPE hrExport SYSTEM "{DTD_FILE}">\n'
    first_line, rest = xml.split("\n", 1)
    final_xml = first_line + "\n" + doctype + rest

    with open(XML_FILE, "w", encoding="utf-8") as f:
        f.write(final_xml)

    print(f"Created {XML_FILE} and {DTD_FILE} with {len(rows_data)} rows.")


if __name__ == "__main__":
    main()
