from sqlparse.sql import TokenList
from sqlparse.tokens import Keyword


class CTE(TokenList):
    """A WHERE clause."""
    M_OPEN = Keyword, 'with'
    M_CLOSE = Keyword, 'select'
