from pymongo import MongoClient
#import time
import datetime
from datetime import date
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

##authentication info removed 

def dates(date, num_month):
    """Input: dates(YYYY-MM-DD), number of months you want to count
    output: an array of the dates: the first and last day of each month during the two dates

    This function is important because the later computations all rely on it to get the list of dates 
    if weekly data is needed, just replace "months" with "weeks" in the marked line
    the later functions will compute the weekly date accordingly

    Ideally, the input date should start at the beginning of a month(2014-03-01) so that the visuals later
    can have clearly marked x-axis, but the function also works if the input is a date in the middle a 
    month(2014-03-15), it will return you monthly dates starting from the middle. See the function call returns"""

    date = datetime.strptime(date,'%Y-%m-%d')
    ls =[]
    for i in range(0,num_month+1):
        new_date1 = date + relativedelta(months=+i) #"months" can be replaced by "weeks" to compute weekly data
        new_date2 = new_date1 - timedelta(1)
        new_date2 = new_date2.strftime('%Y-%m-%d')
        new_date1= new_date1.strftime('%Y-%m-%d')
        ls.append(new_date2)
        ls.append(new_date1)
    
    ls = ls[1:-1]
    return ls


def pairwise(it):
    """ Helper function, 
    Input: a list
    Output: Every two elements in that list"""
    it = iter(it)
    while True:
        yield next(it), next(it)

###==========================================================================####
##The following two functions compute total posts every month

def total_posts_for_group (collection, group, start_date, end_date): 
    """Input: collection name, group name, start date(YYYY-MM-DD), end date, inclusive
    Output: returns the number of posts during this period
    """
    m = collection.aggregate([
        {"$match": {"page": group, "comment_published": {'$gte': start_date,"$lt": end_date}}},
        {"$group":{"_id": '$status_id'}},
            {"$group": {"_id": "null","count2": { "$sum": 1 }}}
    ])
    count = []
    for i in m:
        count.append(i['count2'])
    return count[0]

def total_posts_for_group2(collection, group, start_date, num_month):
    """Input: collection name, group name,start date, number of months you want to count
    Output: an array of the monthly posts for this group during the period
    """
    output = []
    
    date_ls = dates (start_date, num_month)
    length_date_ls = len(date_ls)
    
    for a, b in pairwise(date_ls):
        start_date = a
        end_date = b
        output2 = total_posts_for_group(collection, group, start_date, end_date)
        output.append(output2)      
        
    return output


###==========================================================================####
##The following two functions compute total comments every month

def total_comments_for_group (collection, group, start_date, end_date): 
    """Input: Collection name, group name, star_date (YYYY-MM-DD), end_date(YYYY-MM-DD)
    Output: Number of comments (int)
"""
    count = [] 
    m = collection.aggregate([
        {"$match": {"page": group, "comment_published": {'$gte': start_date,"$lt": end_date}}},
        {"$group":{"_id": '$comment_id'}},
            {"$group": {"_id": "null","count2": { "$sum": 1 }}}
    ])
    for i in m:
        count.append(i["count2"])
    return count[0]

def total_comments_for_group2 (collection, group, start_date, num_month):
    """Input: Collection name, group name, start_date (YYYY-MM-DD), number of months you want to 
    count from the start_date
    Output: An array of monthly comments
    """
    output = [] 
    date_ls = dates (start_date, num_month)
    length_date_ls = len(date_ls)
    
    for a, b in pairwise(date_ls):
        start_date = a
        end_date = b
        output2 = total_comments_for_group(collection, group, start_date, end_date)
        output.append(output2)      
        
    return output

###==========================================================================####
##The following two functions compute total comments every month

def total_commenters_for_group (collection, group, start_date, end_date): 
    """Input: collection name, group name, start_date, end_date the dates should be in the 
    format YYYY-MM-DD
     Output: an integer representing the total distinct commenters in this group 
     during these two dates, inclusive"""
    m = collection.aggregate([
        {"$match": {"page": group, "comment_published": {'$gte': start_date,"$lt": end_date}}},
        {"$group":{"_id": '$author_id'}},
            {"$group": {"_id": "null","count2": { "$sum": 1 }}}
    ])
    count = []
    for i in m:
        count.append(i['count2'])
    return count[0]

def total_commenters_for_group2 (collection, group, start_date, num_month): 
    """Input: Collection name, group name, start date, and how many months you 
    want to count from the first starting date
    Output: An array of total commenter number for each month
 """
    output = []
    
    date_ls = dates (start_date, num_month)
    length_date_ls = len(date_ls)
    
    for a, b in pairwise(date_ls):
        start_date = a
        end_date = b
        output2 = total_commenters_for_group(collection, group, start_date, end_date)
        output.append(output2)      
        
    return output



###==========================================================================####
##The following two functions compute the new commenters every month

def new_commenters_in_group (collection, group, start_date0, start_date, end_date):
    """Input: collection name, group name, start_date0 is the date you want to use as a reference, 
start_date is the start date you want to check for the new commenters. 
end_date is the last day you want to check for the new commenters. 
the dates are all inclusive for the computation. 

Output: an integer of the new commenters during start_date and end_date with the reference date
as start_date0

Example: suppose there are 10000 people made comments during 2016-04-01(start_date) and 2016-04-30(end_date)
but of these 10000 people, 8000 have made comments during 2016-01-01(start_date0) and 2016-03-30
the output is 2000. That is, 2000 people are the new commenters
"""
    m = collection.aggregate([
        {"$match": {"page": group, "comment_published": {'$gte': start_date0,"$lt": end_date}}},
        {"$group":{"_id": '$author_id'}}])
    total_commenter = []
    for i in m:
        total_commenter.append(i['_id'])
    
    #count all commenters from start_date0 to the previous month
    n = collection.aggregate([
        {"$match": {"page": group, "comment_published": {'$gte': start_date0,"$lt": start_date}}},
        {"$group":{"_id": '$author_id'}}])
    previous_commenter =[]
    for i in n:
        previous_commenter.append(i['_id'])
    
    #substract the second group from the first group
    previous_commenter = set(previous_commenter)
    new_commenter = [x for x in total_commenter if x not in previous_commenter]
    
    return len(new_commenter)

def new_commenters_in_group2 (collection, group, start_date0, start_date, num_month):
    """Input: collection name, group, start_date(YYYY-MM-DD), number of months you want to 
        count from the starting date
        Output: an integer array that stores the monthly count of new commenters. """
    start_date0 = start_date0
    
    output = []
    
    date_ls = dates(start_date, num_month)
    length_date_ls = len(date_ls)
    
    for a, b in pairwise(date_ls):
        start_date = a
        end_date = b
        output2 = new_commenters_in_group(collection, group, start_date0, start_date, end_date)
        output.append(output2)      
        
    return output

if __name__ == '__main__':
    collection = fc
    start_date0 = '2015-01-01'
    start_date = '2015-03-01'
    num = 3
    page1 = 'officialbritainfirst'

    print("start executing...")
    posts = total_posts_for_group2(collection, page1, start_date, num)
    comments = total_comments_for_group2(collection, page1, start_date, num)
    commenters = total_commenters_for_group2(collection, page1, start_date, num)
    new_commenters = new_commenters_in_group2(collection, page1, start_date0, start_date, num)

    print("Starting from ", start_date, "the next", num, "months' monthly posts are: ", posts)
    print("Starting from ", start_date, "the next", num, "months' monthly comments are: ", comments)
    print("Starting from ", start_date, "the next", num, "months' monthly commenters are: ", commenters)
    print("Starting from ", start_date, "the next", num, 
      "months' new commenters for each month are:", new_commenters)
    print("Done!")


