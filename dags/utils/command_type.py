from enum import Enum

class CommandType(Enum):
    UPDATE_DATABASE_XML = 'read xml insert db'
    UPDATE_FILE = 'apply changeset to pbf'
    UPDATE_DATABASE_PBF = 'read pbf insert db'
