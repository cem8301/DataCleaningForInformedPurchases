# Scrape
import scrapy
from scrapy import Request
import re, csv

class JobsSpider(scrapy.Spider):
   
   # Setup the search
   name = "vans"
   allowed_domains = ["craigslist.org"]

   # List of search options
   query = 'vans'
   search_distance=100
   postal=06033
   min_price=50
   max_price=10000
   auto_make_model='subaru'
   min_auto_year=1995
   max_auto_year=3000
   min_auto_miles=0
   max_auto_miles=15000
   condition=10  # 10=new,20=likenew,30=excellent,40=good,50=fair,60=salvage
   auto_cylinders=4
   auto_drivetrain=1 # 1=fwd, 2=rwd, 3=4wd
   auto_fuel_type=1 # 1=gas, 2=diesel, 3=hybrid, 4=electric, 5=other
   auto_size=3 # 1=compact, 2=full-size, 3=mid-size, 4=sub-compact
   auto_title_status=1 # 1=clean, 2=salvage, 3=rebuilt, 4=parts only, 5=lien, 6=missing
   auto_transmission=2 # 1=manual, 2=automatic, 3=other
   auto_bodytype=1 # 1=bus, 2=convertible, 3=coupe, 4=hatchback, 5=mini-van, 6=offroad, 
                   # 7=pickup, 8=sedan, 9=truck, 10=SUV, 11=wagon, 12=van, 13=other
   
   # Craigslist start URL
   start_urls = ["https://newyork.craigslist.org/search/cto?min_price=1000"]
   


   def parse(self, response):
      cars = response.xpath('//p[@class="result-info"]')

      # Print number of cars per page
      total = len(cars) + 1
      print(str(total)+' ..............')

      # Iterate over cars
      for car in cars:
         relative_url = car.xpath('a/@href').extract_first()
         absolute_url = response.urljoin(relative_url)
         title = car.xpath('a/text()').extract_first()
         address = car.xpath('span[@class="result-meta"]/span[@class="result-hood"]/text()').extract_first("")[2:-1]

         yield Request(absolute_url, callback=self.parse_page, meta={'URL': absolute_url, 'Title': title, 'Address':address})
      try:
         relative_next_url = response.xpath('//a[@class="button next"]/@href').extract_first()
         absolute_next_url = "https://newyork.craigslist.org" + relative_next_url

         yield Request(absolute_next_url, callback=self.parse)
      except:
         pass



   def parse_page(self, response):
     
      # Needed lists
      all_makes= ['bertone', 'london coach co inc', 'isis imports ltd', 'import trade services', 'lexus', 'jba motorcars, inc.', 'merkur', 'gmc', 'yugo', 'lotus', 'saleen', 'asc incorporated', 'coda automotive', 'byd', 'smart', 'dodge', 'am general', 'aurora cars ltd', 'honda', 'aston martin', 'panther car company limited', 'srt', 'spyker', 'mercedes-benz', 'jaguar', 'saturn', 'geo', 'daewoo', 'lincoln', 'bill dovell motor car company', 'pas, inc', 'nissan', 'pas inc - gmc', 'roush performance', 'saab', 'fisker', 'vector', 'excalibur autos', 'volga associated automobile', 'bentley', 'texas coach company', 'toyota', 'import foreign auto sales inc', 'ruf automobile gmbh', 'volvo', 'general motors', 'pontiac', 'pagani', 'quantum technologies', 'panos', 'qvale', 'lambda control systems', 'mobility ventures llc', 'j.k. motors', 's and s coach company  e.p. dutton', 'hummer', 'eagle', 'morgan', 'isuzu', 'hyundai', 'volkswagen', 'mazda', 'wallace environmental', 'american motors corporation', 'consulier industries inc', 'rolls-royce', 'bugatti', 'fiat', 'acura', 'renault', 'jeep', 'daihatsu', 'mahindra', 'land rover', 'chrysler', 'infiniti', 'panoz auto-development', 'avanti motor corporation', 'subaru', 'buick', 'mercury', 'porsche', 'kenyon corporation of america', 'federal coach', 'cx automotive', 'environmental rsch and devp corp', 'red shift ltd.', 'genesis', 'ccc engineering', 'mclaren automotive', 'evans automobiles', 'bitter gmbh and co. kg', 'ford', 'vpg', 'bmw', 'mitsubishi', 'vixen motor company', 'tvr engineering ltd', 'lamborghini', 'koenigsegg', 'pininfarina', 'suzuki', 'mcevoy motors', 'london taxi', 'plymouth', 'maybach', 'oldsmobile', 'goldacre', 'mini', 'tesla', 'ram', 'superior coaches div e.p. dutton', 'audi', 'maserati', 'shelby', 'chevrolet', 'e. p. dutton, inc.', 'dabryan coach builders inc', 'sterling', 'grumman olson', 'dacia', 'scion', 'cadillac', 'saleen performance', 'kia', 'peugeot', 'alfa romeo', 'azure dynamics', 'laforza automobile inc', 'tecstar, lp', 'ferrari', 'karma', 'grumman allied industries', 'bmw alpina', 'autokraft limited', 'chevy'] # added chevy
      
      all_years = ['1985', '1986', '1987', '1989', '1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019']

      # Make dictionary
      makes = {}
      with open('/home/carolyn/Documents/Classes/CSCI E-63 Big Data Analytics/final_project/practice_joins/fuel_simple.csv') as csvfile:
         rows = csv.reader(csvfile, delimiter=',')
         # Dont read the header
         next(rows)
         for row in rows:
            # 0:cylinders,1:displ,2:drive,3:fueltype,4:make,5:model,6:ucity,7:uhighway,8:trany,9:vclass,10:year
            make_ = row[4].lower()
            year_ = int(row[10])
            model_ = row[5].lower()
            if make_ in makes:
               if year_ in makes[make_]:
                  if model_ in makes[make_][year_]:
                     pass
                  else:
                     makes[make_][year_].append(model_)
               else: 
                  makes[make_][year_] = [model_]
            else:
               makes[make_] = {year_ : [model_]}

      # Get pieces
      url = response.meta.get('URL')
      title = response.meta.get('Title')
      address = response.meta.get('Address')

      # Get set tags: title, price, description. Clean up text
      title = response.xpath('//*[@ id = "titletextonly"]/text()').extract_first().replace(',', '')
      price_init = response.xpath('//*[@class = "price"]/text()').extract_first()
      if(price_init):      
         price = price_init.replace('$', '').replace(',', '')
      else:
         price = ''
      description = "".join(line for line in response.xpath('//*[@id="postingbody"]/text()').extract()).replace(',', '').replace('\n', '')
     

      # Get all vehicle information
      given_tags = response.xpath('//p[@class="attrgroup"]/span/text()').extract()
      given_values = response.xpath('//p[@class="attrgroup"]/span/b/text()').extract()
      
      # Many times the car title is given as a tag_value, which throughs off the dictionary. Check.
      given_tags_clean = filter(None,[x.replace('\n', '').replace(' ', '').replace(',', '') for x in given_tags])
      given_values_clean = filter(None,[x.replace('\n', '').replace(',', '') for x in given_values])
      # Make sure the tag placement is correct
      if ( len(given_values_clean) > len(given_tags_clean) or given_tags_clean[-1] == "otherpostings"):
         dictionary = dict(zip(given_tags_clean, given_values_clean[1::]))
         title2 = given_values_clean[0]
      else:
         dictionary = dict(zip(given_tags_clean, given_values_clean))   
         title2 = ''

      # VIN
      # odometer
      # name
      # condition
      # cylinders
      # drive
      # fuel
      # paint color
      # size
      # title status
      # transmission
      # type
      # model year      
      vin = dictionary[u'VIN:'] if u'VIN:' in given_tags_clean else ''
      odometer = dictionary[u'odometer:'] if u'odometer:' in given_tags_clean else ''
      condition = dictionary[u'condition:'] if u'condition:' in given_tags_clean else ''
      cylinders = dictionary[u'cylinders:'] if u'cylinders:' in given_tags_clean else ''
      cylinders = re.sub("[^0-9]+","",cylinders)
      drive = dictionary[u'drive:'] if u'drive:' in given_tags_clean else ''
      fuel = dictionary[u'fuel:'] if u'fuel:' in given_tags_clean else ''
      paint_color = dictionary[u'paintcolor:'] if u'paintcolor:' in given_tags_clean else ''
      size = dictionary[u'size:'] if u'size:' in given_tags_clean else ''
      title_status = dictionary[u'titlestatus:'] if u'titlestatus:' in given_tags_clean else ''
      transmission = dictionary[u'transmission:'] if u'transmission:' in given_tags_clean else '' 
      vtype = dictionary[u'type:'] if u'type:' in given_tags_clean else ''
      year = dictionary[u'modelyear:'] if u'modelyear:' in given_tags_clean else ''


      # Get make, model, and year are filled out. Important for join with fuel.csv
      # Check title, name, and description
      search_all = title+title2+drive+description
      search_title = list(set([re.sub("[^0-9a-zA-Z- ]+","",x.lower()) for x in title.split(' ')]))
      search_all_clean = re.sub("[^0-9a-zA-Z ]+","",search_all.lower())
      search_list = list(set([re.sub("[^0-9a-zA-Z ]+","",x.lower()) for x in search_all.split(' ')]))      

      # Make
      make_title = all_makes + search_title
      make_all = all_makes + search_list 
      match_title = [x for x in make_title if make_title.count(x) > 1] 
      match_all = [x for x in make_all if make_all.count(x) > 1] 
      # Check the title first, becasuse ofter there is excess information in the description
      if( len(match_title) > 0 ):
         make = match_title[0]
         if( make == 'chevy'):
            make = 'chevrolet'
      # Check all text
      elif( len(match_all) > 0 ):
         make = match_all[0]
         if( make == 'chevy'):
            make = 'chevrolet'      
      else:
         make = None

      # Year
      if( year == ''):
         year_title = all_years + search_title
         year_all = all_years + search_list 
         match_title = [x for x in year_title if year_title.count(x) > 1] 
         match_all = [x for x in year_all if year_all.count(x) > 1] 
         # Check the title first, becasuse ofter there is excess information in the description
         if( len(match_title) > 0 ):
            year = int(match_title[0])
         # Check all text 
         elif( len(match_all) > 0 ):
            year = int(match_all[0])
         else:
            year = None
      else:
         # Year should be an int
         year = int(year.replace(' ', ''))

      # Model
      # Instead of splitting all words... take out all spaces in both and search for items in text string. 
      model = None
      if( make != None and year != None ):   
         
         # Use try/except incase a make/year has no models
         try:
            models = makes[make][year]
         except:
            models = []

         model_dict = dict.fromkeys(models, 0)
         myBreak = False         

         # Loop over possible models for make/ year to find a match
         for m in models:
            # Method 1
            if ' '+m+' ' in search_all_clean:   
               model = m
               #print("(1)")
               myBreak = True
               break
            else:
               # Method 2
               if( drive != ''):
                  #print("(2)")
                  # Check again. Big issue with models including drive in text:
                  sect = re.split('[^a-zA-Z0-9-/]', m)[0].lower()
                  result = re.search(sect,search_all_clean)
                  if result:
                     newSearch = result.group(0) + '.*' + drive         
                     #print(newSearch)
                     # Check drive result
                     if re.search(newSearch, m):  
                        model = m
                        myBreak = True
                        break
                     # If drive fwd or rwd try 2wd for more matches
                     if( drive == 'fwd' or drive == 'rwd'): 
                        newSearch2 = result.group(0) + '.*' + '2wd'  
                        if re.search(newSearch2, m):  
                           model = m
                           myBreak = True
                           break               
               # Method 3- smart catch
               model_sect = re.split('[^a-zA-Z0-9.!]', m)
               for sect in model_sect:
                  model_dict[m] += search_all_clean.count(' '+sect.lower()+' ')

         if(sum(model_dict.values()) > 0 and myBreak == False):
            array = model_dict.values()
            #print(array)
            # Check if valid. If count <= 2 (since often forget to supply drive type)
            if (array.count(max(array)) <= 2 and max(array) >= 1): 
               model = max(model_dict.iterkeys(), key=lambda k: model_dict[k])
               #print("~~~~~~~~~~~~~~~~~NEW METHOD FOR MODEL~~~~~~~~~~~~~~~~~")
               #print(model_dict)
               #print("~~~~~~~~~~~~~~~~~NEW METHOD FOR MODEL~~~~~~~~~~~~~~~~~")
      
      # Cylinders
      if( cylinders == ''):
         result = re.search('.*(\d{1,2}).{0,3}cylinder',search_all_clean)
         result2 = re.search('.*cylinder.{0,3}(\d{1,2})\s',search_all_clean)
         result3= re.search('.*v(\d{1,2})\s',search_all_clean)
         if(result):
            cylinders = result.group(1)
         elif(result2):
            cylinders = result2.group(1)
         elif(result3):
            cylinders = result3.group(1)
    
      # Only keep good data for simplicaity:
      # Check if successful with make, model, year, transmission, and cylinders
      if(make != None and model != None and year != None and transmission != '' and cylinders != ''):
         yield{'url': url, 'title': title, 'price': price, 'address': address, 'vin': vin, 'odometer': odometer, 'condition': condition, 'cylinders': cylinders, 'drive': drive, 'fuel': fuel, 'paint_color': paint_color, 'size': size, 'title_status': title_status, 'transmission': transmission, 'type': vtype, 'year': year, 'make': make, 'model': model, 'description': description}
      else: 
         pass


