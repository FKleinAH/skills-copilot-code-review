"""
MongoDB database configuration and setup for Mergington High School API
"""

import copy
import os
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from argon2 import PasswordHasher, exceptions as argon2_exceptions


class UpdateResult:
    """Lightweight update result compatible with PyMongo usage in routers."""

    def __init__(self, modified_count: int):
        self.modified_count = modified_count


class InMemoryCollection:
    """Small in-memory collection for local development without MongoDB."""

    def __init__(self):
        self._documents = {}

    def _extract(self, document, key):
        current = document
        for part in key.split("."):
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current

    def _matches(self, document, query):
        for key, expected in query.items():
            actual = self._extract(document, key)
            if isinstance(expected, dict):
                for operator, value in expected.items():
                    if operator == "$in":
                        if isinstance(actual, list):
                            if not any(item in value for item in actual):
                                return False
                        elif actual not in value:
                            return False
                    elif operator == "$gte":
                        if actual is None or actual < value:
                            return False
                    elif operator == "$lte":
                        if actual is None or actual > value:
                            return False
                    else:
                        return False
            elif actual != expected:
                return False
        return True

    def count_documents(self, query):
        return sum(1 for _ in self.find(query))

    def insert_one(self, document):
        doc = copy.deepcopy(document)
        self._documents[doc["_id"]] = doc
        return None

    def find(self, query):
        for document in self._documents.values():
            if self._matches(document, query):
                yield copy.deepcopy(document)

    def find_one(self, query):
        for document in self.find(query):
            return document
        return None

    def update_one(self, query, update):
        for doc_id, document in self._documents.items():
            if not self._matches(document, query):
                continue

            modified = False
            if "$push" in update:
                for field, value in update["$push"].items():
                    current = self._extract(document, field)
                    if isinstance(current, list):
                        current.append(value)
                        modified = True

            if "$pull" in update:
                for field, value in update["$pull"].items():
                    current = self._extract(document, field)
                    if isinstance(current, list) and value in current:
                        current.remove(value)
                        modified = True

            self._documents[doc_id] = document
            return UpdateResult(modified_count=1 if modified else 0)

        return UpdateResult(modified_count=0)

    def aggregate(self, pipeline):
        if len(pipeline) != 3:
            return []

        unwind_stage, group_stage, sort_stage = pipeline
        unwind_field = unwind_stage.get("$unwind", "").lstrip("$")
        group_field = group_stage.get("$group", {}).get("_id", "").lstrip("$")
        sort_field, sort_direction = next(iter(sort_stage.get("$sort", {}).items()))

        if unwind_field != group_field or sort_field != "_id" or sort_direction != 1:
            return []

        unique_values = set()
        for document in self._documents.values():
            values = self._extract(document, unwind_field)
            if isinstance(values, list):
                unique_values.update(values)

        return [{"_id": value} for value in sorted(unique_values)]


def _build_collections():
    """Build MongoDB collections or in-memory fallback collections."""
    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    try:
        mongo_client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=2000)
        mongo_client.admin.command("ping")
        mongo_db = mongo_client["mergington_high"]
        return mongo_client, mongo_db["activities"], mongo_db["teachers"]
    except PyMongoError:
        return None, InMemoryCollection(), InMemoryCollection()


client, activities_collection, teachers_collection = _build_collections()

# Methods


def hash_password(password):
    """Hash password using Argon2"""
    ph = PasswordHasher()
    return ph.hash(password)


def verify_password(hashed_password: str, plain_password: str) -> bool:
    """Verify a plain password against an Argon2 hashed password.

    Returns True when the password matches, False otherwise.
    """
    ph = PasswordHasher()
    try:
        ph.verify(hashed_password, plain_password)
        return True
    except argon2_exceptions.VerifyMismatchError:
        return False
    except Exception:
        # For any other exception (e.g., invalid hash), treat as non-match
        return False


def init_database():
    """Initialize database if empty"""

    # Initialize activities if empty
    if activities_collection.count_documents({}) == 0:
        for name, details in initial_activities.items():
            activities_collection.insert_one({"_id": name, **details})

    # Initialize teacher accounts if empty
    if teachers_collection.count_documents({}) == 0:
        for teacher in initial_teachers:
            teachers_collection.insert_one(
                {"_id": teacher["username"], **teacher})


# Initial database if empty
initial_activities = {
    "Chess Club": {
        "description": "Learn strategies and compete in chess tournaments",
        "schedule": "Mondays and Fridays, 3:15 PM - 4:45 PM",
        "schedule_details": {
            "days": ["Monday", "Friday"],
            "start_time": "15:15",
            "end_time": "16:45"
        },
        "max_participants": 12,
        "participants": ["michael@mergington.edu", "daniel@mergington.edu"]
    },
    "Programming Class": {
        "description": "Learn programming fundamentals and build software projects",
        "schedule": "Tuesdays and Thursdays, 7:00 AM - 8:00 AM",
        "schedule_details": {
            "days": ["Tuesday", "Thursday"],
            "start_time": "07:00",
            "end_time": "08:00"
        },
        "max_participants": 20,
        "participants": ["emma@mergington.edu", "sophia@mergington.edu"]
    },
    "Morning Fitness": {
        "description": "Early morning physical training and exercises",
        "schedule": "Mondays, Wednesdays, Fridays, 6:30 AM - 7:45 AM",
        "schedule_details": {
            "days": ["Monday", "Wednesday", "Friday"],
            "start_time": "06:30",
            "end_time": "07:45"
        },
        "max_participants": 30,
        "participants": ["john@mergington.edu", "olivia@mergington.edu"]
    },
    "Soccer Team": {
        "description": "Join the school soccer team and compete in matches",
        "schedule": "Tuesdays and Thursdays, 3:30 PM - 5:30 PM",
        "schedule_details": {
            "days": ["Tuesday", "Thursday"],
            "start_time": "15:30",
            "end_time": "17:30"
        },
        "max_participants": 22,
        "participants": ["liam@mergington.edu", "noah@mergington.edu"]
    },
    "Basketball Team": {
        "description": "Practice and compete in basketball tournaments",
        "schedule": "Wednesdays and Fridays, 3:15 PM - 5:00 PM",
        "schedule_details": {
            "days": ["Wednesday", "Friday"],
            "start_time": "15:15",
            "end_time": "17:00"
        },
        "max_participants": 15,
        "participants": ["ava@mergington.edu", "mia@mergington.edu"]
    },
    "Art Club": {
        "description": "Explore various art techniques and create masterpieces",
        "schedule": "Thursdays, 3:15 PM - 5:00 PM",
        "schedule_details": {
            "days": ["Thursday"],
            "start_time": "15:15",
            "end_time": "17:00"
        },
        "max_participants": 15,
        "participants": ["amelia@mergington.edu", "harper@mergington.edu"]
    },
    "Drama Club": {
        "description": "Act, direct, and produce plays and performances",
        "schedule": "Mondays and Wednesdays, 3:30 PM - 5:30 PM",
        "schedule_details": {
            "days": ["Monday", "Wednesday"],
            "start_time": "15:30",
            "end_time": "17:30"
        },
        "max_participants": 20,
        "participants": ["ella@mergington.edu", "scarlett@mergington.edu"]
    },
    "Math Club": {
        "description": "Solve challenging problems and prepare for math competitions",
        "schedule": "Tuesdays, 7:15 AM - 8:00 AM",
        "schedule_details": {
            "days": ["Tuesday"],
            "start_time": "07:15",
            "end_time": "08:00"
        },
        "max_participants": 10,
        "participants": ["james@mergington.edu", "benjamin@mergington.edu"]
    },
    "Debate Team": {
        "description": "Develop public speaking and argumentation skills",
        "schedule": "Fridays, 3:30 PM - 5:30 PM",
        "schedule_details": {
            "days": ["Friday"],
            "start_time": "15:30",
            "end_time": "17:30"
        },
        "max_participants": 12,
        "participants": ["charlotte@mergington.edu", "amelia@mergington.edu"]
    },
    "Weekend Robotics Workshop": {
        "description": "Build and program robots in our state-of-the-art workshop",
        "schedule": "Saturdays, 10:00 AM - 2:00 PM",
        "schedule_details": {
            "days": ["Saturday"],
            "start_time": "10:00",
            "end_time": "14:00"
        },
        "max_participants": 15,
        "participants": ["ethan@mergington.edu", "oliver@mergington.edu"]
    },
    "Science Olympiad": {
        "description": "Weekend science competition preparation for regional and state events",
        "schedule": "Saturdays, 1:00 PM - 4:00 PM",
        "schedule_details": {
            "days": ["Saturday"],
            "start_time": "13:00",
            "end_time": "16:00"
        },
        "max_participants": 18,
        "participants": ["isabella@mergington.edu", "lucas@mergington.edu"]
    },
    "Sunday Chess Tournament": {
        "description": "Weekly tournament for serious chess players with rankings",
        "schedule": "Sundays, 2:00 PM - 5:00 PM",
        "schedule_details": {
            "days": ["Sunday"],
            "start_time": "14:00",
            "end_time": "17:00"
        },
        "max_participants": 16,
        "participants": ["william@mergington.edu", "jacob@mergington.edu"]
    }
}

initial_teachers = [
    {
        "username": "mrodriguez",
        "display_name": "Ms. Rodriguez",
        "password": hash_password("art123"),
        "role": "teacher"
    },
    {
        "username": "mchen",
        "display_name": "Mr. Chen",
        "password": hash_password("chess456"),
        "role": "teacher"
    },
    {
        "username": "principal",
        "display_name": "Principal Martinez",
        "password": hash_password("admin789"),
        "role": "admin"
    }
]
