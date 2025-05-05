from pymongo import MongoClient

# Where/$match
def match_stage(conditions):  # input: {condition}
    """Creates a $match stage for filtering documents."""
    return {"$match": conditions} if conditions else {}

# Group by ... having/$group...$match...
# Group By/$group
def group_stage(agg_type, group_by_field, aggregate_field):
    """Creates a $group stage to aggregate data by a specified field."""
    if aggregate_field == 'count':
        return {
            "$group": {
                "_id": f"${group_by_field}",
                "count": {"$sum": 1}
            }
        }
    elif agg_type == 'sum':
        return {
            "$group": {
                "_id": f"${group_by_field}",
                f"total_{aggregate_field}": {"$sum": f"${aggregate_field}"}
            }
        }
        
    elif agg_type == 'avg':
        return {
            "$group": {
                "_id": f"${group_by_field}",
                f"average_{aggregate_field}": {"$avg": f"${aggregate_field}"}
            }
        }


def sort_stage(field, order=-1):
    """Creates a $sort stage to order documents by a specified field."""
    return {"$sort": {field: order}}


# Join On/$lookup
def lookup_stage(from_collection, local_field, foreign_field, as_field):
    """Creates a $lookup stage to join collections."""
    return {
        "$lookup": {
            "from": from_collection,
            "localField": local_field,
            "foreignField": foreign_field,
            "as": as_field
        }
    }
    
    
# projection
def project_stage(field_list):
    if not field_list:  # Handle empty or None field_list
        return {"$project": {"_id": 0}}  # Default: exclude _id only
    
    project_dict = {}
    for field in field_list:
        project_dict[field] = 1
    return {"$project": project_dict}



def build_pipeline(match=None, group=None, sort=None, lookup=None, project=None):
    """Builds the aggregation pipeline with optional stages."""
    pipeline = []
    if match:
        pipeline.append(match)
    if lookup:
        pipeline.append(lookup)
    if group:
        pipeline.append(group)
    if sort:
        pipeline.append(sort)
    if project:
        pipeline.append(project)    
    return pipeline