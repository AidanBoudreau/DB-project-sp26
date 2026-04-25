from MARIADB_CREDS import DB_CONFIG
from mariadb import connect
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist
from models.Item import Item
from models.Rental import Rental
from models.Customer import Customer
from datetime import date, timedelta


conn = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"], host=DB_CONFIG["host"],
               database=DB_CONFIG["database"], port=DB_CONFIG["port"])


cur = conn.cursor()


def add_item(new_item: Item = None):
    """
    new_item - An Item object containing a new item to be inserted into the DB in the item table.
        new_item and its attributes will never be None.
    """

    i_item_sk = 1 #table is empty
    
    # get max item_sk and add 1 for i_item_sk
    cur.execute("SELECT MAX(i_item_sk) FROM Item")
    for row in cur:
        if row[0] is not None:
            i_item_sk = row[0] + 1
    
    # start date from start year
    i_rec_start_date = str(new_item.start_year) + "-01-01"

    # insert query
    query = "INSERT INTO Item VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    cur.execute(query, (i_item_sk, new_item.item_id, i_rec_start_date, new_item.product_name, new_item.brand, None, new_item.category, new_item.manufact, new_item.current_price, new_item.num_owned))

def add_customer(new_customer: Customer = None):
    """
    new_customer - A Customer object containing a new customer to be inserted into the DB in the customer table.
        new_customer and its attributes will never be None.
    """
    customer_sk = 1 #table is empty
    
    # get max c_sk and add 1 for c_customer_sk
    cur.execute("SELECT MAX(c_customer_sk) FROM Customer")
    for row in cur:
        if row[0] is not None:
            customer_sk = row[0] + 1

    address_sk = 1 #table is empty

    # get max c_sk and add 1 for ca_address_sk
    cur.execute("SELECT MAX(ca_address_sk) FROM customer_address")
    for row in cur:
        if row[0] is not None:
            address_sk = row[0] + 1

    # get customer first & last name
    first_last = new_customer.name.split(" ", 1)
    first_name = first_last[0]
    if len(first_last)>1:
        last_name = first_last[1]
    else:
        last_name = ""

    # tokenize address
    tokens = new_customer.address.split(", ")
    street = tokens[0].split(" ", 1)
    street_num = street[0]
    if len(street)>1:
        street_name = street[1]
    else:
        street_name = ""
    city = tokens[1]
    state_zip = tokens[2].split(" ")
    state = state_zip[0]
    zip = state_zip[1]

    query = "INSERT INTO Customer VALUES (?, ?, ?, ?, ?, ?)"
    params = (customer_sk, new_customer.customer_id, first_name, last_name, new_customer.email, address_sk)
    cur.execute(query, params)

    query = "INSERT INTO customer_address VALUES (?, ?, ?, ?, ?, ?)"
    params = (address_sk, street_num, street_name, city, state, zip)
    cur.execute(query, params)

def edit_customer(original_customer_id: str = None, new_customer: Customer = None):
    """
    original_customer_id - A string containing the customer id for the customer to be edited.
    new_customer - A Customer object containing attributes to update. If an attribute is None, it should not be altered.
    """
    customer_changes = []
    params_customer_changes = []
    address_changes = []
    params_address_changes = []
    
    # changing customer id
    if new_customer.customer_id is not None:
        customer_changes.append("c_customer_id = ?")
        params_customer_changes.append(new_customer.customer_id)

    # changing customer name
    if new_customer.name is not None:
        # tokenize name, append name change and params respectively
        first_last = new_customer.name.split(" ", 1)
        first_name = first_last[0]
        if len(first_last)>1:
            last_name = first_last[1]
        else:
            last_name = ""
        customer_changes.append("c_first_name = ?")
        params_customer_changes.append(first_name)
        customer_changes.append("c_last_name = ?")
        params_customer_changes.append(last_name)
        
    # changing customer address
    if new_customer.address is not None:
        # tokenize address, append address change and params respectively
        tokens = new_customer.address.split(", ")
        street = tokens[0].split(" ", 1)
        street_number = street[0]
        if len(street)>1:
            street_name = street[1]
        else:
            street_name = ""
        city = tokens[1]
        state_zip = tokens[2].split(" ")
        state = state_zip[0]
        zip = state_zip[1]
        address_changes.extend(["ca_street_number = ?", "ca_street_name = ?", "ca_city = ?", "ca_state = ?", "ca_zip = ?"])
        params_address_changes.extend([street_number, street_name, city, state, zip])

    # changing customer email
    if new_customer.email is not None:
        customer_changes.append("c_email_address = ?")
        params_customer_changes.append(new_customer.email)

    # UPDATE Customer SET c_customer_id, c_first_name, etc WHERE c_customer_id = original_customer_id
    if customer_changes:
        query = "UPDATE Customer SET "
        for i in range(len(customer_changes)-1):
            query = query + customer_changes[i] + ", "
        query = query + customer_changes[-1] + " WHERE c_customer_id = ?"
        params_customer_changes.append(original_customer_id)
        cur.execute(query, params_customer_changes)
    
    # UPDATE customer_address SET ca_street_number, ca_street_name, etc WHERE ca_address_sk = (SELECT c_current_addr_sk FROM Customer WHERE c_customer_id = original_customer_id)
    if address_changes:
        query = "UPDATE customer_address SET "
        for i in range(len(address_changes)-1):
            query = query + address_changes[i] + ", "
        query = query + address_changes[-1] + " WHERE ca_address_sk = (SELECT c_current_addr_sk FROM Customer WHERE c_customer_id = ?)"
        params_address_changes.append(original_customer_id)
        cur.execute(query, params_address_changes)


def rent_item(item_id: str = None, customer_id: str = None):
    """
    item_id - A string containing the Item ID for the item being rented.
    customer_id - A string containing the customer id of the customer renting the item.
    """
    raise NotImplementedError("you must implement this function")


def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's new place in line.
    """
    raise NotImplementedError("you must implement this function")

def update_waitlist(item_id: str = None):
    """
    Removes person at position 1 and shifts everyone else down by 1.
    """
    raise NotImplementedError("you must implement this function")


def return_item(item_id: str = None, customer_id: str = None):
    """
    Moves a rental from rental to rental_history with return_date = today.
    """
    # get rental_date and due_date from primary keys (item_id, customer_id)
    query = "SELECT rental_date, due_date FROM rental WHERE item_id = ? AND customer_id = ?"
    params = (item_id, customer_id)
    cur.execute(query, params)
    
    for row in cur:
        rental_date = row[0]
        due_date = row[1]

    # get return date from todays date    
    return_date = date.today()
    
    # Insert data into rental history
    query = "INSERT INTO rental_history VALUES (?, ?, ?, ?, ?)"
    params = (item_id, customer_id, rental_date, due_date, return_date)
    cur.execute(query, params)

    # Remove data from rental
    query = "DELETE FROM rental WHERE item_id = ? AND customer_id = ?"
    params = (item_id, customer_id)
    cur.execute(query, params)


def grant_extension(item_id: str = None, customer_id: str = None):
    """
    Adds 14 days to the due_date.
    """
    # set new due date from adding 14 days to old due date
    query = "SELECT due_date FROM rental WHERE item_id = ? AND customer_id = ?"
    params = (item_id, customer_id)
    cur.execute(query, params)
    
    for row in cur:
        new_due_date = row[0] + timedelta(days=14)
    
    # UPDATE rental to new due date
    query = "UPDATE rental SET due_date = ? WHERE item_id = ? AND customer_id = ?"
    params = (new_due_date, item_id, customer_id)
    cur.execute(query, params)

def get_filtered_items(filter_attributes: Item = None,
                       use_patterns: bool = False,
                       min_price: float = -1,
                       max_price: float = -1,
                       min_start_year: int = -1,
                       max_start_year: int = -1) -> list[Item]:
    """
    Returns a list of Item objects matching the filters.
    """
    query = "SELECT * FROM Item"
    params = []

    # append filtering conditions and params to fill "?" during cur.execute
    if filter_attributes is not None and filter_attributes.item_id is not None:
            query = query + " WHERE i_item_id = ?"
            params.append(filter_attributes.item_id)
    cur.execute(query, params)

    # create items (Data Model) from executed query rows & columns in schema
    items = []
    for row in cur:
        items.append(Item(item_id       = row[1], 
                          product_name  = row[3], 
                          brand         = row[4], 
                          category      = row[6], 
                          manufact      = row[7], 
                          current_price = row[8], 
                          start_year    = row[2].year, 
                          num_owned     = row[9]
                          ))
    return items

def get_filtered_customers(filter_attributes: Customer = None, use_patterns: bool = False) -> list[Customer]:
    """
    Returns a list of Customer objects matching the filters.
    """
    # join customer on customer address to get all info in one row
    query = "SELECT Customer.c_customer_id, Customer.c_first_name, Customer.c_last_name, customer_address.ca_street_number, customer_address.ca_street_name, customer_address.ca_city, customer_address.ca_state, customer_address.ca_zip, Customer.c_email_address From Customer" \
    " JOIN customer_address ON Customer.c_current_addr_sk = customer_address.ca_address_sk"
    params = []

    # get customer data from customer id
    if filter_attributes is not None and filter_attributes.customer_id is not None:
        query = query + " WHERE Customer.c_customer_id = ?"
        params.append(filter_attributes.customer_id) 

    cur.execute(query, params)

    # create Customer (Data Model) from executed query. create name and address from tokens.
    customers=[]
    for row in cur:
        name = row[1] + " " + row[2]
        address = row[3] + " " + row[4] + ", " + row[5] + ", " + row[6] + " " + row[7]
        customers.append(Customer(customer_id=row[0], name=name, address=address, email=row[8]))
    return customers


def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    """
    Returns a list of Rental objects matching the filters.
    """
    # get attributes from rental
    query = "SELECT * FROM rental"
    params = []

    # append conditions and params by the requested item and customer ids
    if filter_attributes is not None:
        conditions = []
        if filter_attributes.item_id is not None:
            conditions.append("item_id = ?")
            params.append(filter_attributes.item_id)
        if filter_attributes.customer_id is not None:
            conditions.append("customer_id = ?")
            params.append(filter_attributes.customer_id)

        # add " WHERE item_id = i_id AND customer_id = c_id"
        if conditions:
            query = query + " WHERE "
            for i in range(len(conditions)-1):
                query = query + conditions[i] + " AND "
            query = query + conditions[-1]
    
    cur.execute(query, params)

    # create Rental (Data Model) from query, return to helper_functions.py
    rentals=[]
    for row in cur:
        rentals.append(Rental(item_id=row[0], customer_id=row[1], rental_date=str(row[2]), due_date=str(row[3])))
    return rentals


def get_filtered_rental_histories(filter_attributes: RentalHistory = None,
                                  min_rental_date: str = None,
                                  max_rental_date: str = None,
                                  min_due_date: str = None,
                                  max_due_date: str = None,
                                  min_return_date: str = None,
                                  max_return_date: str = None) -> list[RentalHistory]:
    """
    Returns a list of RentalHistory objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    """
    Returns a list of Waitlist objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def number_in_stock(item_id: str = None) -> int:
    """
    Returns num_owned - active rentals. Returns -1 if item doesn't exist.
    """
    raise NotImplementedError("you must implement this function")


def place_in_line(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's place_in_line, or -1 if not on waitlist.
    """
    raise NotImplementedError("you must implement this function")


def line_length(item_id: str = None) -> int:
    """
    Returns how many people are on the waitlist for this item.
    """
    raise NotImplementedError("you must implement this function")


def save_changes():
    """
    Commits all changes made to the db.
    """
    conn.commit()


def close_connection():
    """
    Closes the cursor and connection.
    """
    cur.close()
    conn.close()

