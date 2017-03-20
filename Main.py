from Scrape import getData, getCSV

links = getData()
for link in links['yellowLinks']:
    print link
for link in links['greenLinks']:
    print link

info = getCSV(links['greenLinks'][0])
print info.pop()