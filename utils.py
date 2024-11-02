import datetime
import json

def get_extracted_data(item, property):
    property = json.loads(property)

    licenses = {}
    for license in item.get("agency").get("licenses", []):  # Default to an empty list if "licenses" is missing
        licenses[license['authority']] = license['number']
    externalID = item.get("externalID")
    href = f"https://www.bayut.com/property/details-{externalID}.html"
    json_data = {
        "id": item.get("id"),
        "ownerID": item.get("ownerID"),
        "title": item.get("title"),
        "baths": item.get("baths"),
        "rooms": item.get("rooms"),
        "price": item.get("price"),
        "createdAt": datetime.datetime.fromtimestamp(item.get("createdAt")),
        "updatedAt": datetime.datetime.fromtimestamp(item.get("updatedAt")),
        "reactivatedAt": datetime.datetime.fromtimestamp(item.get("reactivatedAt")),
        "area": item.get("area"),
        "plotArea": item.get("plotArea"),
        "location": ",".join(sub_loc.get("name") for sub_loc in reversed(item.get("location"))),
        "category": ",".join(sub_cat.get("name") for sub_cat in item.get("category")),
        "mobile": item.get("phoneNumber").get("mobile"),
        "phone": item.get("phoneNumber").get("phone"),
        "whatsapp": item.get("phoneNumber").get("whatsapp"),
        "proxyPhone": item.get("phoneNumber").get("proxyPhone"),
        "contactName": item.get("contactName"),  # Agent
        "permitNumber": item.get("permitNumber"),
        "ded": licenses.get("DED") if licenses.get("DED") else "",
        "rera": licenses.get("RERA") if licenses.get("RERA") else "",
        "orn": licenses.get("ORN") if licenses.get("ORN") else "",
        "type": property.get("Type") if property.get("Type") else "",
        "purpose": property.get("Purpose") if property.get("Purpose") else "",
        "reference_no": "Bayut-" + item.get("referenceNumber"),
        "completion": property.get("Completion") if property.get("Completion") else "",
        "furnishing": property.get("Furnishing") if property.get("Furnishing") else "",
        "truCheck": property.get("TruCheck™") if property.get("TruCheck™") else "",
        "added_on": property.get("Added on") if property.get("Added on") else "",
        "handover_date": property.get("Handover date") if property.get("Handover date") else "",
        "description": property.get("description"),
        "size": property.get("size"),
        "building_name": property.get("Building Name"),
        "park_spaces": property.get("Total Parking Spaces"),
        "floors": property.get("Total Floors"),
        "building_area": property.get("Total Building Area"),
        "swimming_pools": property.get("Swimming Pools"),
        "elevators": property.get("Elevators"),
        "offices": property.get("Offices"),
        "shops": property.get("Shops"),
        "developers": property.get("Developer"),
        "built_up_Area": property.get("Built-up Area"),
        "usage": property.get("Usage"),
        "retail_centres" : property.get("Retail Centres"),
        "parking_availability": property.get("Parking Availability"),
        "ownership": property.get("Ownership"),
        "ownerAgent": item.get("ownerAgent").get("name"),
        "agency": item.get("agency").get("name"),
        "property_link": href
    }
    return json_data

def get_raw_data(page_num, purpose, category, search):
    request_params = f"page={page_num}&hitsPerPage=24&query=&optionalWords=&facets=%5B%5D&maxValuesPerFacet=10&attributesToHighlight=%5B%5D&attributesToRetrieve=%5B%22type%22%2C%22agency%22%2C%22area%22%2C%22baths%22%2C%22category%22%2C%22contactName%22%2C%22externalID%22%2C%22sourceID%22%2C%22id%22%2C%22location%22%2C%22objectID%22%2C%22phoneNumber%22%2C%22coverPhoto%22%2C%22photoCount%22%2C%22price%22%2C%22product%22%2C%22productLabel%22%2C%22purpose%22%2C%22geography%22%2C%22permitNumber%22%2C%22referenceNumber%22%2C%22rentFrequency%22%2C%22rooms%22%2C%22slug%22%2C%22slug_l1%22%2C%22slug_l2%22%2C%22slug_l3%22%2C%22title%22%2C%22title_l1%22%2C%22title_l2%22%2C%22title_l3%22%2C%22createdAt%22%2C%22updatedAt%22%2C%22ownerID%22%2C%22isVerified%22%2C%22propertyTour%22%2C%22verification%22%2C%22completionDetails%22%2C%22completionStatus%22%2C%22furnishingStatus%22%2C%22-agency.tier%22%2C%22requiresLogin%22%2C%22coverVideo%22%2C%22videoCount%22%2C%22description%22%2C%22description_l1%22%2C%22description_l2%22%2C%22description_l3%22%2C%22descriptionTranslated%22%2C%22descriptionTranslated_l1%22%2C%22descriptionTranslated_l2%22%2C%22descriptionTranslated_l3%22%2C%22floorPlanID%22%2C%22panoramaCount%22%2C%22hasMatchingFloorPlans%22%2C%22hasTransactionHistory%22%2C%22state%22%2C%22photoIDs%22%2C%22reactivatedAt%22%2C%22hidePrice%22%2C%22extraFields%22%2C%22projectNumber%22%2C%22locationPurposeTier%22%2C%22ownerAgent%22%2C%22hasEmail%22%2C%22plotArea%22%2C%22offplanDetails%22%2C%22paymentPlans%22%2C%22paymentPlanSummaries%22%2C%22availabilityStatus%22%2C%22userExternalID%22%2C%22units%22%2C%22unitCategories%22%2C%22downPayment%22%5D&filters=purpose%3A%22{purpose}%22%20AND%20category.slug%3A%22{category}%22&numericFilters="
    if search:
        request_params = f"page={page_num}&hitsPerPage=24&query=&optionalWords=&facets=%5B%5D&maxValuesPerFacet=10&attributesToHighlight=%5B%5D&attributesToRetrieve=%5B%22type%22%2C%22agency%22%2C%22area%22%2C%22baths%22%2C%22category%22%2C%22contactName%22%2C%22externalID%22%2C%22sourceID%22%2C%22id%22%2C%22location%22%2C%22objectID%22%2C%22phoneNumber%22%2C%22coverPhoto%22%2C%22photoCount%22%2C%22price%22%2C%22product%22%2C%22productLabel%22%2C%22purpose%22%2C%22geography%22%2C%22permitNumber%22%2C%22referenceNumber%22%2C%22rentFrequency%22%2C%22rooms%22%2C%22slug%22%2C%22slug_l1%22%2C%22slug_l2%22%2C%22slug_l3%22%2C%22title%22%2C%22title_l1%22%2C%22title_l2%22%2C%22title_l3%22%2C%22createdAt%22%2C%22updatedAt%22%2C%22ownerID%22%2C%22isVerified%22%2C%22propertyTour%22%2C%22verification%22%2C%22completionDetails%22%2C%22completionStatus%22%2C%22furnishingStatus%22%2C%22-agency.tier%22%2C%22requiresLogin%22%2C%22coverVideo%22%2C%22videoCount%22%2C%22description%22%2C%22description_l1%22%2C%22description_l2%22%2C%22description_l3%22%2C%22descriptionTranslated%22%2C%22descriptionTranslated_l1%22%2C%22descriptionTranslated_l2%22%2C%22descriptionTranslated_l3%22%2C%22floorPlanID%22%2C%22panoramaCount%22%2C%22hasMatchingFloorPlans%22%2C%22hasTransactionHistory%22%2C%22state%22%2C%22photoIDs%22%2C%22reactivatedAt%22%2C%22hidePrice%22%2C%22extraFields%22%2C%22projectNumber%22%2C%22locationPurposeTier%22%2C%22ownerAgent%22%2C%22hasEmail%22%2C%22plotArea%22%2C%22offplanDetails%22%2C%22paymentPlans%22%2C%22paymentPlanSummaries%22%2C%22availabilityStatus%22%2C%22userExternalID%22%2C%22units%22%2C%22unitCategories%22%2C%22downPayment%22%5D&filters=purpose%3A%22{purpose}%22%20AND%20(location.slug%3A%22%2F{search}%22)%20AND%20category.slug%3A%22{category}%22&numericFilters="
    raw_data = {
        "requests": [
            {
                "indexName": "bayut-production-ads-date-desc-en",
                "params": request_params
            }
        ]
    }
    return raw_data

def get_params():
    params = {
        "x-algolia-agent": "Algolia for JavaScript (3.35.1); Browser (lite)",
        "x-algolia-application-id": "LL8IZ711CS",
        "x-algolia-api-key": "15cb8b0a2d2d435c6613111d860ecfc5"
    }
    return params

def get_headers():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'Content-Type': 'application/json',
    }
    return headers

def get_request_url():
    url = "https://ll8iz711cs-1.algolianet.com/1/indexes/*/queries"
    return url