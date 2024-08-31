# 1. send get request to eventbrite API
# 2. send data to bigquery
from EBtoken import key, Org_id
import requests


def list_eventbrite_events():
    endpoint = f'https://www.eventbriteapi.com/v3/organizations/{Org_id}/events/'

    headers = {
        'Authorization': f'Bearer {key}',
    }
    events = []
    response = requests.get(endpoint, headers=headers)
       # Continue looping as long as the response is successful (status code 200)
    while response.status_code == 200:
        data = response.json()
        events.extend(data['events'])
        print(f"Fetched {len(data['events'])} events...")  # Debugging: Check number of events fetched per page

        
        # Check if 'pagination' exists and if 'has_more_items' is true
        if 'pagination' in data and data['pagination'].get('has_more_items', False):
            # Get the continuation token
            continuation_token = data['pagination'].get('continuation')
            if continuation_token:
                # Update the endpoint URL with the continuation token as a query parameter
                next_endpoint = f'{endpoint}?continuation={continuation_token}'
                response = requests.get(next_endpoint, headers=headers)
            else:
                print('No continuation URL')
                break  # No continuation URL, exit the loop
        else:
            print('No pagination')
            break  # No more items or pagination not present, exit the loop
    
    response.raise_for_status()  # Raise an error for bad status codes
    return events

def get_attendee_data(event_id):
    endpoint = f'https://www.eventbriteapi.com/v3/events/{event_id}/attendees/'
    headers = {
        'Authorization': f'Bearer {key}',
    }
    attendees = []
    response = requests.get(endpoint, headers=headers)
    
    # Continue looping as long as the response is successful (status code 200)
    while response.status_code == 200:
        data = response.json()
        
        # Loop through attendees and collect all their data
        for attendee in data['attendees']:
            attendees.append(attendee)
        
        # Handle pagination if there are more pages of attendees
        if 'pagination' in data and data['pagination'].get('has_more_items', False):
            continuation_url = data['pagination'].get('continuation_url')
            if continuation_url:
                response = requests.get(continuation_url, headers=headers)
            else:
                break  # No continuation URL, exit the loop
        else:
            break  # No more items, exit the loop
    
    response.raise_for_status()  # Raise an error for bad status codes
    return attendees

def list_attendees(event_id):
    endpoint = f'https://www.eventbriteapi.com/v3/events/{event_id}/attendees/'
    headers = {
        'Authorization': f'Bearer {key}',
    }

    attendees = []

    response = requests.get(endpoint, headers=headers)
       # Continue looping as long as the response is successful (status code 200)
    while response.status_code == 200:
        data = response.json()
        
        # Loop through attendees and collect their first and last names
        for attendee in data['attendees']:
            first_name = attendee['profile']['first_name']
            last_name = attendee['profile']['last_name']
            attendees.append({'first_name': first_name, 'last_name': last_name})
        
        # Handle pagination if there are more pages of attendees
        if 'pagination' in data and data['pagination'].get('has_more_items', False):
            continuation_url = data['pagination'].get('continuation_url')
            if continuation_url:
                response = requests.get(continuation_url, headers=headers)
            else:
                break  # No continuation URL, exit the loop
        else:
            break  # No more items, exit the loop
    
    response.raise_for_status()  # Raise an error for bad status codes
    return attendees


def print_event_names(events):
    for event in events:
        # Accessing the event name from the 'name' field
        event_name = event['name']['text']
        print(f"Event Name: {event_name}")

def print_attendees(events):
    attendee_data_list = []

    for event in events:
            attendees_data = get_attendee_data(event['id'])
            attendee_data_list.extend(attendees_data)
            for attendee in attendee_data_list:
                print("Attendee Details:")
                print("==================")
                
                print(f"Created: {attendee.get('created')}")
                print(f"Last Changed: {attendee.get('changed')}")
                print(f"Ticket Class ID: {attendee.get('ticket_class_id')}")
                print(f"Variant ID: {attendee.get('variant_id')}")
                print(f"Ticket Class Name: {attendee.get('ticket_class_name')}")
                print(f"Quantity: {attendee.get('quantity')}")
                print(f"Costs: {attendee.get('costs')}")
                print(f"Profile: {attendee.get('profile')}")
                print(f"Addresses: {attendee.get('addresses')}")
                print(f"Questions: {attendee.get('questions')}")
                print(f"Answers: {attendee.get('answers')}")
                print(f"Barcodes: {attendee.get('barcodes')}")
                print(f"Team: {attendee.get('team')}")
                print(f"Affiliate: {attendee.get('affiliate')}")
                print(f"Checked In: {attendee.get('checked_in')}")
                print(f"Cancelled: {attendee.get('cancelled')}")
                print(f"Refunded: {attendee.get('refunded')}")
                print(f"Status: {attendee.get('status')}")
                print(f"Event ID: {attendee.get('event_id')}")
                print(f"Order ID: {attendee.get('order_id')}")
                print(f"Guest List ID: {attendee.get('guestlist_id')}")
                print(f"Invited By: {attendee.get('invited_by')}")
                print(f"Delivery Method: {attendee.get('delivery_method')}")
                
                print('\n')

def retrieve_full_history():
    try:
        events = list_eventbrite_events() #list of events
        # print_event_names(events)
        # print_attendees(events)

        attendees_list = []
        for event in events:
            event_attendees = get_attendee_data(event['id'])
            attendees_list.extend(event_attendees)
        
        load_data(attendees_list)
     
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except Exception as err:
        print(f"An error occurred: {err}")

def load_data(data):
    print('load data')

def main():
    # retrieve latest event id - from event function and from segment platform. Use both as a check.
    events_list = list_eventbrite_events()
    latest_event = events_list.pop()
    event_id = latest_event['id']
    # print(latest_event)
    attendees_list = get_attendee_data(event_id)
    load_data(attendees_list)
    

if __name__ == "__main__":
    main()
    # retrieve_full_history()

    
