from unittest.mock import Mock, MagicMock


def sqlalchemy_row_to_dict(rep):
    return {
        x.name: getattr(rep, x.name)
        for x in rep.__table__.columns
        if x not in ('created_at', 'updated_at')
    }


# __table__.columns = ('col1 value', 'col2 value', 'col3 value', 'some datetime', 'another datetime')


rep = Mock()
# rep = ('col1', 'col2', 'col3', 'created_at', 'updated_at')
# print(rep.name)

rep.__table__ = Mock()
rep.__table__.columns = Mock().side_effect
rep.__table__.columns = ('col1 value', 'col2 value', 'col3 value', 'some datetime', 'another datetime')
rep.__table__.columns.name = Mock().side_effect
rep.__table__.columns.name = ('col1', 'col2', 'col3', 'created_at', 'updated_at')
# rep.__table__.columns.name = ('col1', 'col2', 'col3', 'created_at', 'updated_at')
# rep.__getattr__ = Mock()
# rep.__getattr__ = MagicMock
# print(getattr(rep, ))

a = sqlalchemy_row_to_dict(rep)
print(a)
