"""

Important note: This Python script's work is not isolated.
Upon certain actions, the database will call various trigger functions.
Actions resulting from trigger functions are commented in the following syntax:
    # ~~ basic function description in pseudo code
"""

import json

import fetch
from _scrape_and_parse import OwnerName
from _constants import DASHES, MEDIUM_DASHES, SHORT_DASHES, SPACE
from _constants import DEFAULT_PROP_UNIT, BOT_ID


def parcel_not_in_db(parid, db_cursor):
    select_sql = """
        SELECT parid FROM property
        WHERE parid = %s"""
    # TODO: ALLOW DUPLICATE PAIRDS?
    db_cursor.execute(select_sql, [parid])
    row = db_cursor.fetchone()
    if row is None:
        print("Parcel {} not in database.".format(parid))
        return True
    return False


def write_property_to_db(imap, db_cursor):
    # Todo: Write function in a way so that we can reuse the insert sql for the alter sql
    insert_sql = """
        INSERT INTO property(
            propertyid, municipality_municode, parid, lotandblock,
            address, usegroup, constructiontype, countycode,
            notes, addr_city, addr_state, addr_zip,
            ownercode, propclass, lastupdated, lastupdatedby,
            locationdescription, bobsource_sourceid, creationts            
        )
        VALUES(
            DEFAULT, %(municipality_municode)s, %(parid)s, %(lotandblock)s,
            %(address)s, %(usegroup)s, %(constructiontype)s, %(countycode)s,
            %(notes)s, %(addr_city)s, %(addr_state)s, %(addr_zip)s,
            %(ownercode)s, %(propclass)s, now(), %(lastupdatedby)s, %(locationdescription)s,
            %(bobsource)s, now()
        )
        RETURNING propertyid;
    """
    db_cursor.execute(insert_sql, imap)
    return db_cursor.fetchone()[0]  # Returns the property_id


def update_property_in_db(propid, imap, db_cursor):
    # Todo: Write function in a way so that we can reuse the insert sql for the alter sql
    imap["propertyid"] = propid
    insert_sql = """
        UPDATE property SET(
            municipality_municode = %(municipality_municode)s,
            parid = %(parid)s,
            lotandblock = %(lotandblock)s,
            address = %(address)s,
            usegroup = %(usegroup)s,
            constructiontype = %(constructiontype)s,
            countycode = %(countycode)s,
            notes = %(notes)s,
            addr_city = %(addr_city)s,
            addr_state = %(addr_state)s,
            addr_zip = %(addr_zip)s,
            ownercode = %(ownercode)s,
            propclass = %(propclass)s,
            lastupdated = now(),
            lastupdatedby = %(lastupdatedby)s,
            locationdescription) = %(locationdescription)s,
            bobsource = %(bobsource)s
        )
        WHERE propertyid = %(propertyid)
    """
    db_cursor.execute(insert_sql, imap)
    return db_cursor.fetchone()[0]  # Returns the property_id


def create_insertmap_from_record(r):
    """

    Arguments:
        r: dict
            The dictionized JSON record for a parcel id provided by the WPRDC
    Returns:
        dict
    """
    # Todo: This is a mess. Go over with others
    imap = {}
    # imap["propertyid"] = None
    imap["municipality_municode"] = r["MUNICODE"]
    imap["parid"] = r["PARID"]
    # imap["lotandblock"] = extract_lotandblock_from_parid(imap["PARID"])
    imap["usegroup"] = r[
        "USEDESC"
    ]  # ? I THINK this is what we want? Example, MUNICIPAL GOVERMENT.
    imap["constructiontype"] = None  # ?
    imap["countycode"] = None  # ? 02
    imap["notes"] = "Data from the WPRDC API"

    imap["lotandblock"] = ""  # TODO: FIGURE OUT LOT AND BLOCK

    # TODO: MAKE SURE THIS IS THE CORRECT DATA
    imap["address"] = SPACE.join((r["PROPERTYHOUSENUM"], r["PROPERTYADDRESS"]))
    imap["address_extra"] = r["PROPERTYFRACTION"]  # TODO: Add column
    imap["addr_city"] = r["PROPERTYCITY"]
    imap["addr_state"] = r["PROPERTYSTATE"]
    imap["addr_zip"] = r["PROPERTYZIP"]
    imap["ownercode"] = r[
        "OWNERCODE"
    ]  # Todo: Verify there is an ownercode to ownerdesc table
    imap["propclass"] = r["CLASS"]  # Todo: Verify
    imap["lastupdatedby"] = BOT_ID
    imap["locationdescription"] = None
    imap["bobsource"] = None
    return imap


def create_cecase_insertmap(prop_id, unit_id):
    imap = {}
    imap["cecasepubliccc"] = 111111
    imap["property_propertyid"] = prop_id
    imap["propertyunit_unitid"] = unit_id
    imap["login_userid"] = BOT_ID
    imap["casename"] = "Import from county site"
    imap["casephase"] = None
    imap["notes"] = "Initial case for each property"
    imap["paccenabled"] = False
    imap["allowuplinkaccess"] = None
    imap["propertyinfocase"] = True
    imap["personinfocase_personid"] = None
    imap["bobsource_sourceid"] = None
    imap["active"] = True
    return imap


def write_cecase_to_db(cecase_map, db_cursor):
    insert_sql = """INSERT INTO public.cecase(
        caseid, cecasepubliccc, property_propertyid, propertyunit_unitid,
        login_userid, casename, casephase, originationdate,
        closingdate, creationtimestamp, notes, paccenabled,
        allowuplinkaccess, propertyinfocase, personinfocase_personid, bobsource_sourceid,
        active
    )
    VALUES(
        DEFAULT, %(cecasepubliccc)s, %(property_propertyid)s, %(propertyunit_unitid)s,
        %(login_userid)s, %(casename)s, cast ('Closed' as casephase), now(),
        now(), now(), %(notes)s, %(paccenabled)s,
        %(allowuplinkaccess)s, %(propertyinfocase)s, %(personinfocase_personid)s, %(bobsource_sourceid)s,
        %(active)s
    )
    RETURNING caseid"""
    db_cursor.execute(insert_sql, cecase_map)
    return db_cursor.fetchone()[0]  # caseid
    # print("Writen CE Case")


def create_owner_insertmap(name, r):
    imap = {}
    imap["muni_municode"] = r["MUNICODE"]

    imap["jobtitle"] = None
    imap["phonecell"] = None
    imap["phonehome"] = None
    imap["phonework"] = None
    imap["email"] = None
    # TODO: Change our database to match theirs

    imap["mailing1"] = r["CHANGENOTICEADDRESS1"]
    imap["mailing2"] = r["CHANGENOTICEADDRESS2"]
    imap["mailing3"] = r["CHANGENOTICEADDRESS3"]
    imap["mailing4"] = r["CHANGENOTICEADDRESS4"]

    # Todo: Deprecate
    imap["address_street"] = r["CHANGENOTICEADDRESS1"]
    imap["address_city"] = r["CHANGENOTICEADDRESS3"].rstrip(" PA")
    imap["address_state"] = "PA"
    imap["address_zip"] = r["CHANGENOTICEADDRESS4"]
    imap[
        "notes"
    ] = "In case of confusion, check automated record entry with raw text from the county database."
    imap["expirydate"] = None
    imap["isactive"] = True
    imap["isunder18"] = None
    imap["humanverifiedby"] = None
    imap["rawname"] = name.raw
    imap["cleanname"] = name.clean
    imap["fname"] = name.first
    imap["lname"] = name.last
    imap["multientity"] = name.multientity
    imap["compositelname"] = name.compositelname
    return imap


def write_person_to_db(record, db_cursor):
    insert_sql = """
        INSERT INTO public.person(
            persontype, muni_municode, fname, lname, 
            jobtitle, phonecell, phonehome, phonework, 
            email, address_street, address_city, address_state, 
            address_zip, notes, lastupdated, expirydate, 
            isactive, isunder18, humanverifiedby, rawname,
            cleanname, compositelname, multientity)
        VALUES(
            cast ( 'ownercntylookup' as persontype), %(muni_municode)s, %(fname)s, %(lname)s,
            %(jobtitle)s, %(phonecell)s, %(phonehome)s, %(phonework)s,
            %(email)s, %(address_street)s, %(address_city)s, %(address_state)s,
            %(address_zip)s, %(notes)s, now(), %(expirydate)s,
            %(isactive)s, %(isunder18)s, %(humanverifiedby)s, %(rawname)s,
            %(cleanname)s, %(compositelname)s, %(multientity)s
        )
        RETURNING personid;
    """
    db_cursor.execute(insert_sql, record)
    return db_cursor.fetchone()[0]


def connect_property_to_person(prop_id, person_id, db_cursor):
    propperson = {"prop_id": prop_id, "person_id": person_id}
    insert_sql = """
        INSERT INTO public.propertyperson(
            property_propertyid, person_personid    
        )
        VALUES(
            %(prop_id)s, %(person_id)s
        );
    """
    db_cursor.execute(insert_sql, propperson)


def create_propertyexternaldata_map(prop_id, name, r):
    # Yes, this is basically duplicate code.
    # However, explicitly restating what record data maps to insert data makes the code easier to both read and write.
    imap = {}
    imap["property_propertyid"] = prop_id
    imap["ownername"] = name
    imap["address_street"] = SPACE.join((r["PROPERTYHOUSENUM"], r["PROPERTYADDRESS"]))
    imap["address_city"] = r["PROPERTYCITY"]
    imap["address_state"] = "PA"
    imap["address_zip"] = r["PROPERTYZIP"]
    imap["address_citystatezip"] = SPACE.join(
        (imap["address_city"], imap["address_state"], imap["address_zip"])
    )
    imap["saleprice"] = r["SALEPRICE"]
    imap["saledate"] = r["SALEDATE"]  # Todo: Add column to databse
    try:
        imap["saleyear"] = r["SALEDATE"][-4:]
    except TypeError:
        imap["saleyear"] = None
    imap["assessedlandvalue"] = r["COUNTYLAND"]
    imap["assessedbuildingvalue"] = r["COUNTYBUILDING"]
    imap["assessmentyear"] = r[
        "TAXYEAR"
    ]  # BIG TODO: IMPORTANT: Scrape assessment year from county
    imap["usecode"] = r["USECODE"]
    imap["livingarea"] = r["FINISHEDLIVINGAREA"]
    imap["condition"] = r["CONDITION"]  # Todo: Condition to condition desc table
    imap["taxstatus"] = r["TAXCODE"]
    imap["taxstatusyear"] = r["TAXYEAR"]
    imap["notes"] = SPACE.join(("Scraped by bot", BOT_ID))
    return imap
    # imap["lastupdated"]


def write_propertyexternaldata(propextern_map, db_cursor):
    insert_sql = """
        INSERT INTO public.propertyexternaldata(
        extdataid,
        property_propertyid, ownername, address_street, address_citystatezip,
        address_city, address_state, address_zip, saleprice,
        saleyear, assessedlandvalue, assessedbuildingvalue, assessmentyear,
        usecode, livingarea, condition, taxstatus,
        taxstatusyear, notes, lastupdated
        )
        VALUES(
            DEFAULT,
            %(property_propertyid)s, %(ownername)s, %(address_street)s, %(address_citystatezip)s,
            %(address_city)s, %(address_state)s, %(address_zip)s, %(saleprice)s,
            %(saleyear)s, %(assessedlandvalue)s, %(assessedbuildingvalue)s, %(assessmentyear)s,
            %(usecode)s, %(livingarea)s, %(condition)s, %(taxstatus)s,
            %(taxstatusyear)s, %(notes)s, now()
        )
        RETURNING property_propertyid;
    """
    db_cursor.execute(insert_sql, propextern_map)
    return db_cursor.fetchone()[0]  # property_id


def parcel_changed(prop_id, db_cursor):
    """ Checks if parcel info is different from last time"""
    select_sql = """
        SELECT(
            property_propertyid, ownername, address_street, address_citystatezip,
            livingarea, condition, taxstatus
        )
            FROM public.propertyexternaldata
            WHERE property_propertyid = %(prop_id)s
            ORDER BY lastupdated DESC
            LIMIT 2;
    """

    db_cursor.execute(select_sql, {"prop_id": prop_id})
    selection = db_cursor.fetchall()
    try:
        if selection[0] == selection[1]:
            return False
        print(
            "Property ",
            prop_id,
            "'s propertyexternaldata is different from last time.",
            sep="",
        )
        return True
    except IndexError:  # If this is the first time the property_propertyid occurs in propertyexternaldata
        print("First time parcel has appeared in propertyexternaldata")
        return True


def create_unit_map(prop_id, unit_id):
    imap = {}
    imap["property_propertyid"] = prop_id
    imap["default_unit"] = unit_id
    return imap


def insert_unit(imap, db_cursor):
    insert_sql = """
        INSERT INTO public.propertyunit(
            unitid, unitnumber, property_propertyid, otherknownaddress, notes, 
            rental)
        VALUES(
            DEFAULT, %(default_unit)s, %(property_propertyid)s, NULL, 
            'robot-generated unit representing the primary habitable dwelling on a property', 
            FALSE)
        RETURNING unitid;
    """
    db_cursor.execute(insert_sql, imap)
    return db_cursor.fetchone()[0]  # unit_id


def get_cecase_from_db(prop_id, db_cursor):
    caseid_sql = """
    SELECT caseid FROM cecase
        JOIN property ON cecase.property_propertyid = property.propertyid
        WHERE propertyid = %s;
    """
    db_cursor.execute(caseid_sql, [prop_id])
    # TODO: MAKE SURE CECASE ACTUALLY WORKS
    # For example, can there be multiple caseids for a single property? If so, this breaks
    try:
        return db_cursor.fetchone()[0]  # Case ID
    except TypeError:  # 'NoneType' object is not subscriptable:
        return None


def create_PropertyInfoChange_imap(cecase_id):
    imap = {}
    imap["cecase_caseid"] = cecase_id
    imap["category_catid"] = 300  # Property Info Update
    imap["eventdescription"] = "Change in column"  # Todo: Write better description
    imap["creator_userid"] = BOT_ID
    imap["lastupdatedby_userid"] = BOT_ID
    return imap


def writePropertyInfoChangeEvent(cvu_map, db_cursor):
    # TODO: Create different category id's depending on what is different (name vs tax, etc)
    insert_sql = """
        INSERT INTO public.event(
            category_catid, cecase_caseid, creationts,
            eventdescription, creator_userid, 
            timestart, timeend, lastupdatedby_userid, lastupdatedts
        )
        VALUES(
            %(category_catid)s, %(cecase_caseid)s, now(),
            %(eventdescription)s, %(creator_userid)s,
            now(), now(), %(lastupdatedby_userid)s, now()
        )"""
    db_cursor.execute(insert_sql, cvu_map)


def get_propid(parid, db_cursor):
    select_sql = """
        SELECT propertyid FROM public.property
        WHERE parid = %s;"""
    db_cursor.execute(select_sql, [parid])
    return db_cursor.fetchone()[0]  # property id


def get_unitid_from_db(prop_id, db_cursor):
    # Throws a TypeError if the cursor doesn't return anything
    select_sql = """
        SELECT unitid FROM propertyunit
        WHERE property_propertyid = %s"""
    db_cursor.execute(select_sql, [prop_id])
    try:
        return db_cursor.fetchone()[0]  # unit id
    except TypeError:
        return None


def update_muni(muni, db_cursor, commit=True):
    """
    The core functionality of the script.
    """

    print("Updating {} ({})".format(muni.name, muni.municode))
    print(MEDIUM_DASHES)
    # We COULD not save the file and work only in JSON, but saving the file is better for understanding what happened
    filename = fetch.fetch_muni_data_and_write_to_file(muni)
    if not fetch.validate_muni_json(filename):
        print(DASHES)
        return

    with open(filename, "r") as f:
        file = json.load(f)
        records = file["result"]["records"]

    # Debugging tools
    record_count = 0
    inserted_count = 0
    updated_count = 0

    for record in records:
        inserted_flag = False

        parid = record["PARID"]
        if parcel_not_in_db(parid, db_cursor):
            imap = create_insertmap_from_record(record)
            prop_id = write_property_to_db(imap, db_cursor)

            # TODO: Put code block in function
            if record["PROPERTYUNIT"] != "":
                unit_map = create_unit_map(prop_id, unit_id=record["PROPERTYUNIT"])
            else:
                unit_map = create_unit_map(prop_id, unit_id=DEFAULT_PROP_UNIT)
            unit_id = insert_unit(unit_map, db_cursor)

            cecase_map = create_cecase_insertmap(prop_id, unit_id)
            write_cecase_to_db(cecase_map, db_cursor)

            # Unfortunately, we have to scrape this oursevles to check for changes
            owner_name = OwnerName.get_Owner_given_parid(parid)
            owner_map = create_owner_insertmap(owner_name, record)
            person_id = write_person_to_db(owner_map, db_cursor)
            # ~~ Update Spelling (Not implemented)

            connect_property_to_person(prop_id, person_id, db_cursor)
            inserted_count += 1
            inserted_flag = True
        else:
            prop_id = get_propid(parid, db_cursor)
            # We have to scrape this again to see if it changed
            owner_name = OwnerName.get_Owner_given_parid(parid)

        propextern_map = create_propertyexternaldata_map(
            prop_id, owner_name.raw, record
        )
        # Property external data is a misnomer. It's just a log of the data from every time stuff
        prop_id = write_propertyexternaldata(propextern_map, db_cursor)

        if parcel_changed(prop_id, db_cursor):
            #   This whole block of code does one important task:
            #       writeCodeViolationEvent(cvu_map, db_cursor)
            #   The prefacing code is just error handling to make sure there is enough info to write about it

            # # Code enforcement officers ought to verify data before it is updated in the database.
            # imap = create_insertmap_from_record(record)
            # update_property_in_db(prop_id, imap, db_cursor)
            # TODO: Add newPropertyEvent

            cecase_id = get_cecase_from_db(prop_id, db_cursor)
            if cecase_id is None:
                unit_id = get_unitid_from_db(prop_id, db_cursor)
                if unit_id is None:
                    # TODO: Repeated code. Put in function
                    if record["PROPERTYUNIT"] != "":
                        unit_map = create_unit_map(
                            prop_id, unit_id=record["PROPERTYUNIT"]
                        )
                    else:
                        unit_map = create_unit_map(prop_id, unit_id=DEFAULT_PROP_UNIT)
                    unit_id = insert_unit(unit_map, db_cursor)

                cecase_map = create_cecase_insertmap(prop_id, unit_id)
                cecase_id = write_cecase_to_db(cecase_map, db_cursor)

            cvu_map = create_PropertyInfoChange_imap(cecase_id)
            writePropertyInfoChangeEvent(cvu_map, db_cursor)
            if not inserted_flag:
                updated_count += 1

        if commit:
            db_cursor.commit()

        record_count += 1
        print("Record count:\t", record_count, sep="")
        print("Inserted count:\t", inserted_count, sep="")
        print("Updated count:\t", updated_count, sep="")
        print(SHORT_DASHES)
    print(DASHES)
