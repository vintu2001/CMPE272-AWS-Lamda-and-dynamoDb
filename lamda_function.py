
import os, json, decimal
import boto3
from botocore.exceptions import ClientError


TABLE_NAME = os.getenv("TABLE_NAME", "StudentRecords")
table = boto3.resource("dynamodb").Table(TABLE_NAME)


class DJSON(json.JSONEncoder):
   def default(self, o):
       if isinstance(o, decimal.Decimal):
           return int(o) if o % 1 == 0 else float(o)
       return super().default(o)


def resp(status, body=None):
   if isinstance(body, (dict, list)) or body is None:
       body = json.dumps(body or {}, cls=DJSON)
   # if body is a plain string, send it as-is (useful for the assignment's sample outputs)
   return {"statusCode": status, "body": body}


def body_json(event):
   b = event.get("body")
   if not b:
       return {}
   try:
       return json.loads(b)
   except json.JSONDecodeError:
       raise ValueError("Invalid JSON")


def sid(event):
   p = (event.get("pathParameters") or {}).get("student_id")
   if p: return p
   q = (event.get("queryStringParameters") or {}).get("student_id")
   if q: return q
   return body_json(event).get("student_id")


def create(event):
   data = body_json(event)
   for f in ("student_id", "name", "course"):
       if not data.get(f):
           return resp(400, f"Missing {f}")
   try:
       table.put_item(Item=data, ConditionExpression="attribute_not_exists(student_id)")
       # Match assignment-style output: 200 + plain success string
       return resp(200, "Student record added successfully")
   except ClientError as e:
       if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
           return resp(409, "Student already exists")
       raise


def read(event):
   s = sid(event)
   if not s:
       return resp(400, "student_id is required")
   item = table.get_item(Key={"student_id": s}).get("Item")
   if not item:
       return resp(404, "Student not found")
   return resp(200, item)


def update(event):
   s = sid(event)
   if not s:
       return resp(400, "student_id is required")
   data = body_json(event)
   data.pop("student_id", None)
   if not data:
       return resp(400, "No fields provided")
   names, values, parts = {}, {}, []
   for i, (k, v) in enumerate(data.items(), 1):
       nk, vk = f"#f{i}", f":v{i}"
       names[nk] = k; values[vk] = v
       parts.append(f"{nk} = {vk}")
   expr = "SET " + ", ".join(parts)
   try:
       table.update_item(
           Key={"student_id": s},
           UpdateExpression=expr,
           ExpressionAttributeNames=names,
           ExpressionAttributeValues=values,
           ConditionExpression="attribute_exists(student_id)",
           ReturnValues="NONE",
       )
       return resp(200, "Student record updated successfully")
   except ClientError as e:
       if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
           return resp(404, "Student not found")
       raise


def delete(event):
   s = sid(event)
   if not s:
       return resp(400, "student_id is required")
   try:
       table.delete_item(
           Key={"student_id": s},
           ConditionExpression="attribute_exists(student_id)",
           ReturnValues="NONE",
       )
       return resp(200, "Student record deleted successfully")
   except ClientError as e:
       if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
           return resp(404, "Student not found")
       raise


def lambda_handler(event, context):
   try:
       m = (event.get("httpMethod") or "").upper()
       if m == "POST":    return create(event)
       if m == "GET":     return read(event)
       if m == "PUT":     return update(event)
       if m == "DELETE":  return delete(event)
       return resp(405, "Method not allowed")
   except ValueError as ve:
       return resp(400, str(ve))
   except Exception:
       return resp(500, "Internal server error")
