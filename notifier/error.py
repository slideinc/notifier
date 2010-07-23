#
#
#
'''error

Definitions for access/service return code errors/exceptions.
'''
import exceptions

SUCCESS        = 0
#
# old style, pass through rc values
#
UNKNOWN        = 1
DUPLICATE_KEY  = 2
EXEC_TRACEBACK = 5
AFFINITY_ERROR = 6
#
# new style exceptions.
#
table  = {}
lookup = lambda i, *a: table.get(i, AccessError)(*a)

ACCESS_ERROR_MASK = 0x400 #starting at 1K to avoid collision.

class AccessError(exceptions.Exception):
    id = 0x400 + 0

class DatabaseUnavailable(AccessError):
    '''DatabaseUnavailable

    Database was unavailable to service the request
    '''
    id = 0x400 + 1

class NoServiceHandler(AccessError):
    '''NoServiceHandler

    The requested service handler does not exist.
    '''
    id = 0x400 + 2

class ServiceTraceback(AccessError):
    '''ServiceTraceback

    Unknown/Unhandled exception occured while executing the request.
    '''
    id = 0x400 + 3

class LockTimeout(AccessError):
    '''LockTimeout

    resource lock timed out/heavy lock contention
    '''
    id = 0x400 + 4

class ParameterError(AccessError):
    '''ParameterError

    The request had incorrect/inconsistent parameters.
    '''
    id = 0x400 + 5

class NoServiceDefined(AccessError):
    '''NoServiceDefined

    The request was made with no service defined.
    '''
    id = 0x400 + 6
#
# Graph API
#
GRAPH_ACCESS_ERROR_MASK = 0x3000 # graph errors begin at 12288

class GraphError(AccessError):
    '''base exception for the Graph access api related exceptions'''

class NodeDoesNotExist(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 0

class NodeHasNoChildren(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 1

class ChildExists(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 2

class ParentDoesNotExist(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 5

class TargetIsParent(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 6

class ParentDoesNotMatch(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 7

class EdgeDoesNotExist(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 8

class KeyDoesNotExist(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 9

class InvalidNodeKlass(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 10

class InvalidRoot(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 15

class CannotPerform(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 16

class NodeIsRoot(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 20

class PropertyNotAnInteger(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 40

class IncrementDeltaRequired(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 42

class NodeContention(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 50

class ExpectedValueMismatch(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 55

class MethodNotAllowedForTransaction(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 80

class MaxTransactionLengthExceeded(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 81

class MaxCreateExceeded(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 82

class MaxRemoveExceeded(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 83

class MaxCreateDepthExceeded(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 84

class GraphUnknownError(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 100

class GraphUnimplemented(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 110

class GraphToManyRaceRetries(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 120

class GraphRetry(GraphError):
    id = GRAPH_ACCESS_ERROR_MASK + 121


#
# Inventory API
#
INVENTORY_ACCESS_ERROR_MASK = 0x4000

class InventoryError(AccessError):
    '''base exception for the Inventory access api related exceptions'''

class ElementDoesNotExist(InventoryError):
    id = INVENTORY_ACCESS_ERROR_MASK + 0

#
# Channel API
#
CHANNEL_ACCESS_ERROR_MASK = 0x800

class ChannelError(AccessError):
    id = CHANNEL_ACCESS_ERROR_MASK + 0

class ChannelUpdateInconsistent(ChannelError):
    id = CHANNEL_ACCESS_ERROR_MASK + 1

#
# Sort API
#
SORT_ACCESS_ERROR_MASK = 0x5000

class SortAccessError(AccessError):
    id = SORT_ACCESS_ERROR_MASK + 0

class InvalidStore(SortAccessError):
    id = SORT_ACCESS_ERROR_MASK + 1

class InvalidQuerySortKey(SortAccessError):
    id = SORT_ACCESS_ERROR_MASK + 2

class SortObjectNotFound(SortAccessError):
    id = SORT_ACCESS_ERROR_MASK + 3

class SortUnimplemented(SortAccessError):
    id = SORT_ACCESS_ERROR_MASK + 4

class InsufficientSortRecord(SortAccessError):
    id = SORT_ACCESS_ERROR_MASK + 5

#
# Build ID/exception table
#
for v in locals().values():
    try:
        if issubclass(v, AccessError):
            table[v.id] = v
    except TypeError:
        pass

table[None] = AccessError
#
# end..
