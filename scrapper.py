import json, requests,csv

def getDataFromAPI(offset):
    url = 'https://api.bazaarvoice.com/data/batch.json?passkey=e8bg3vobqj42squnih3a60fui&apiversion=5.5&displaycode=6543-en_us&resource.q0=reviews&filter.q0=isratingsonly%3Aeq%3Afalse&filter.q0=productid%3Aeq%3Adev5800066&filter.q0=contentlocale%3Aeq%3Aen_US&sort.q0=rating%3Adesc&stats.q0=reviews&filteredstats.q0=reviews&include.q0=comments&filter_reviews.q0=contentlocale%3Aeq%3Aen_US&filter_reviewcomments.q0=contentlocale%3Aeq%3Aen_US&filter_comments.q0=contentlocale%3Aeq%3Aen_US&limit.q0=100&offset.q0='+str(offset)
    #print(url)
    resp = requests.get(url=url)
    data = json.loads(resp.text)
    return data

def getTotalRecords(data):
    totalResults = data['BatchedResults']['q0']['TotalResults']
    return totalResults

def getRecordsFromData(data):
    records = data['BatchedResults']['q0']['Results']
    rows = []
    for record in records:
        row = []
        row.append('Samsung Galaxy s7')
        row.append(record['Title'])
        row.append(record['ReviewText'])
        row.append(record['SubmissionTime'])
        row.append(record['UserNickname'])
        rows.append(row)
    return rows

def main():
    with open('data.csv', 'w') as csvfile:
        writer = csv.writer(csvfile)
        data = getDataFromAPI(0)
        totalResults = getTotalRecords(data)
        totalResults = int(totalResults)
        writer.writerow(['Device','Title','ReviewText','SubmissionTime','UserNickname'])
        for offset in range(0,totalResults,100):
            print("Getting data from offset: "+str(offset))
            data = getDataFromAPI(offset)
            rows = getRecordsFromData(data)
            #print(rows)
            writer.writerows(rows)
main()