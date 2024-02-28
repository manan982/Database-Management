-- DROP TABLE used to completely remove table structure and associated indexes
-- IF EXISTS used to avoid error on removal if table does not exist
-- CASCADE also removes dependant objects of the table, ensuring if a parent record table is deleted, so is the child record
DROP TABLE IF EXISTS act CASCADE;
DROP TABLE IF EXISTS gig CASCADE;
DROP TABLE IF EXISTS act_gig;
DROP TABLE IF EXISTS gig_ticket;
DROP TABLE IF EXISTS ticket;
DROP TABLE IF EXISTS venue CASCADE; 

-- CREATE TABLE used to create new tables in the database

CREATE TABLE venue (
    venueid SERIAL NOT NULL PRIMARY KEY, -- PRIMARY KEY used to uniquely identify each record. Must be unique. 
    venuename VARCHAR(100) NOT NULL,
    hirecost BIGINT NOT NULL, -- set all column names to be NOT NULL
    capacity BIGINT NOT NULL
);

CREATE TABLE act (
    actid SERIAL PRIMARY KEY,
    actname VARCHAR(100) NOT NULL ,
    genre VARCHAR(20) NOT NULL ,
    members BIGINT NOT NULL ,
    standardfee BIGINT NOT NULL
);

CREATE TABLE gig (
    gigid SERIAL PRIMARY KEY,
    venueid BIGINT REFERENCES venue(venueID) NOT NULL, -- creating FOREIGN KEY that references PRIMARY KEY
    gigtitle VARCHAR(100) NOT NULL,
    gigdate TIMESTAMP NOT NULL,
    gigstatus VARCHAR(10) NOT NULL,
    CONSTRAINT CHK_gig CHECK (gigstatus = 'Cancelled' OR gigstatus = 'GoingAhead') 
    -- CONSTRAINT limits the value that can be placed in the column to only either 'Cancelled' or 'GoingAhead'

);


CREATE TABLE act_gig (
    actid BIGINT REFERENCES act(actID) NOT NULL,
    gigid BIGINT REFERENCES gig(gigID) NOT NULL,
    actfee BIGINT NOT NULL,
    ontime TIMESTAMP NOT NULL,
    duration BIGINT NOT NULL
    CONSTRAINT CHK_act_gig CHECK (duration <= 120) 
    -- CONSTRAINT limits the value that can be placed in the column to less than 120
);


CREATE TABLE gig_ticket (
    gigid BIGINT REFERENCES gig(gigID) NOT NULL,
    pricetype VARCHAR(2) NOT NULL,
    cost BIGINT NOT NULL
);

CREATE TABLE ticket (
    ticketid SERIAL NOT NULL PRIMARY KEY,
    gigid BIGINT NOT NULL REFERENCES gig(gigID),
    pricetype VARCHAR(2) NOT NULL,
    cost BIGINT NOT NULL,
    CustomerName VARCHAR(100),
    CustomerEmail VARCHAR(100)
);


--FUNCTIONS

-- used in OPTION 5
-- Will calculate the tickets needed to be sold of standard type A that would allow the venue and act fees to be paid off
-- takes gigid as input returns required tickets
CREATE FUNCTION reqStantardTkts(IntegerGid BIGINT) RETURNS bigint AS $$ -- method for defining a function
        
        DECLARE requiredTkts BIGINT; -- Required count of tickets that still need selling 
        DECLARE feeLeft BIGINT; -- Amount that stil requires payment    
        DECLARE cumulatedAct BIGINT; -- Cost of all acts 
        DECLARE cumulatedGig BIGINT; -- Complete cost calculated from combining cumulatedAct and cost of hiring the venue

        BEGIN
            SELECT SUM(actfee) INTO cumulatedAct FROM act_gig WHERE IntegerGid = gigid;
            -- Calling sum function, which finds total sum of the actfee column, finding cost of all acts. Store in cumulateedAct

            SELECT (cumulatedAct + venue.hirecost) INTO cumulatedGig FROM venue INNER JOIN gig ON gig.venueid = venue.venueid WHERE IntegerGid = gig.gigid;
            -- INNER JOIN uses to find records that have matching values from tables gig and venue, as long as there is a match in venueid columns. 

            IF ((SELECT SUM(cost) FROM ticket WHERE gigid=IntegerGid) IS NULL) 
            THEN SELECT cumulatedGig INTO feeLeft; -- price to pay for complete Gig if cost from ticket is NULL
            ELSE SELECT ABS(cumulatedGig - SUM(cost)) INTO feeLeft FROM ticket WHERE gigid = IntegerGid;
            --ABS used to calculate absolute value of number. Calculating required fee left.
            END IF; 
            
            -- calculation of dividing payment left by cost of a single ticket to get number of tickets needed to be sold
            IF ((SELECT ABS(feeLeft % cost) FROM gig_ticket WHERE gigid = IntegerGid) <> 0) -- <> means not equal to 
            THEN SELECT ABS(feeLeft / cost) + 1 INTO requiredTkts FROM gig_ticket WHERE pricetype = 'A' AND IntegerGid = gigid; 
            -- case there is a remainder upon division , increment by 1 to ensure no less than combinedGig value is obtained   
            
            ELSE SELECT ABS(feeLeft / cost) INTO requiredTkts FROM gig_ticket WHERE pricetype = 'A' AND IntegerGid = gigid; 
            END IF; -- ensure price type is A, as only want standard tickets 
            
            RETURN requiredTkts; -- return integer number of tickets needed to be sold
        END;
$$ LANGUAGE plpgsql; -- used instead of language sql 


-- used in OPTION 4
-- function checks if the given actname is a headline act of the given gig
-- Takes gigid and actname as input, returns true or false whether headline or not
CREATE FUNCTION hlCheckerOpt4(IntegerGid BIGINT, actualName VARCHAR) RETURNS boolean AS $$

DECLARE IntegerAid BIGINT; -- Stores act id from the given act name

BEGIN 
    SELECT actID FROM ACT INTO IntegerAid WHERE actualName = actname;
    -- Getting the corresponding actID from given actname

    -- Return true if headline act is found. 
    -- Headline is found by checking the actID calculated with the last act in the gig, and these match, then headline
    -- Otherwise, return false 
    -- DESC will sort by descending, and LIMIT 1 will limit the result set to only 1 item, ensuring headline act is chosen
    IF ((SELECT act_gig.actid FROM act_gig WHERE IntegerGid = gigid ORDER BY ontime DESC LIMIT 1) = IntegerAid) 
        THEN RETURN TRUE;
        ELSE RETURN FALSE;
    END IF;
    END;
$$ LANGUAGE plpgsql;

-- used in OPTION 6
-- Alternative version of headline checker used in option 4, but takes act id as input instead of actname
-- also takes gigid as input, again returns true or false whether headline act or not 
CREATE FUNCTION hlChecker(IntegerGid BIGINT, IntegerAid BIGINT) RETURNS boolean AS $$ 
    BEGIN
    
    IF ((SELECT act_gig.actid FROM act_gig WHERE IntegerGid = gigid ORDER BY ontime DESC LIMIT 1) = IntegerAid) 
        THEN RETURN TRUE;
        ELSE RETURN FALSE;
    END IF;
    END;
$$ LANGUAGE plpgsql;

-- used in OPTION 6
-- function verifies if given gig is cancelled or is going ahead. 
-- takes gigid as input, returns either true or false depending on if the gig is going ahead or cancelled
CREATE FUNCTION gaChecker(IntegerGid BIGINT) RETURNS boolean AS $$  
    BEGIN
    IF ((SELECT gigstatus FROM gig WHERE IntegerGid = gig.gigid) = 'GoingAhead') 
    -- checking status
        THEN RETURN TRUE;
        ELSE RETURN FALSE;
    END IF;
    END;
$$ LANGUAGE plpgsql;



-- used in option 8
-- will calcuate the proportion (tickets required / venue capacity), of tickets needed selling
-- Only finding proportion of economically feasible gigs 
-- returns real as division can result in non integer values.
-- Improvement to program could be to allow proportion to be real , rather than integer for increased precision 
-- takes venueid and tickets required as input, returning ticket proportion that have to be sold

CREATE FUNCTION calculation (IntegerVid BIGINT, requiredTkt REAL) RETURNS real AS $$ 
    DECLARE maxTkts BIGINT; -- maxTkts is max tickets that can possible be sold due to venues max cap, obtained from selecting capacity from venue
    BEGIN
        SELECT capacity INTO maxTkts FROM venue WHERE IntegerVid = venueid;
        RETURN ABS(requiredTkt / maxTkts); -- absolute value in case of accidental errors with negative entries
        -- calls function defined below.
    END;
$$ LANGUAGE plpgsql;


--used in option 8
-- function to calculate the number of tickets needed to be economically feasible and break even , else return null
-- ecoomically feasible iff maxTkts is greater than or equal to minTkts, and then can be put in and returned in view
-- add to view notOrdered to be used iff economically feasible.
-- takes venueid, actname and the average price as input. Returns the tickets required for feasbility. 
CREATE FUNCTION requiredTkts(IntegerVid BIGINT, currentAName VARCHAR(100), average BIGINT) RETURNS bigint AS $$

    DECLARE minTkts BIGINT ; -- is least amount of tickets required for economically feasible. Obtained by dividiing combinedPayment by average price
    DECLARE maxTkts BIGINT ; -- maxTkts is max tickets that can possible be sold due to venues max cap, obtained from selecting capacity from venue
    DECLARE combinedPayment BIGINT ; --combinedPayment stores the total cost of hiring the venue and cost of act
    
    BEGIN
     SELECT capacity INTO maxTkts FROM venue WHERE IntegerVid = venueid; -- declaring max venue capacity 
     SELECT (venue.hirecost + act.standardfee) INTO combinedPayment FROM act, venue WHERE IntegerVid = venue.venueid AND currentAName = act.actname;

    -- using ABS(), divide the combined Payment required by average price using %, to find a possible remainder.
    --case there is a remainder, increment by 1 to ensure no less than combinedPayment value is obtained      
    IF (ABS(combinedPayment % average) > 0) THEN minTkts = ABS(combinedPayment / average) + 1; 
    ELSE minTkts = combinedPayment / average ; -- else if no remaineder, than straight whole integer is achieved. Can store calculated value straight
    END IF;
    
    IF (maxTkts >= minTkts) THEN RETURN minTkts; 
    ELSE RETURN NULL;
    END IF;
    
    END;
$$ LANGUAGE plpgsql;


--VIEWS

-- used in OPTION 6
-- This view will store only acts which are of headline and are going ahead. 
CREATE VIEW hlView AS 
    SELECT DISTINCT actname, (ontime::DATE), ticket.ticketid, act_gig.gigid AS thisGigId, act.actid, ticket.CustomerEmail FROM ticket INNER JOIN act_gig ON act_gig.gigid = ticket.gigid INNER JOIN act ON act.actid = act_gig.actid 
    -- Will find the customers attending these acts, and store thier ticket details.
    WHERE gaChecker(act_gig.gigid) AND hlChecker(act_gig.gigid, act_gig.actid);
    -- calling functions that ensures that the act is headline and the act is going ahead
    
-- used in OPTION 6
-- This view will store the year of the act performing as a string
-- uses the function COUNT() to count the number of rows that ticketid has appeared, and store it, along with actname
CREATE VIEW yrUniqueView AS 
    SELECT actname, to_char(ontime, 'YYYY') AS strYr, COUNT(ticketid) AS tktTrack FROM hlView 
    -- to_char converts object of type DATE to characters that can be stored as a string
    GROUP BY to_char(ontime, 'YYYY'), actname; -- will group all data into only these 2 columns 

-- used in OPTION 6
-- For each act, will sum up the total tickets sold annually, using the function SUM()
CREATE VIEW noOfTktsView AS 
    SELECT actname, SUM(tktTrack) as track FROM yrUniqueView 
    GROUP BY actname ;

-- used in OPTION 6
-- Will find total amount of tickets sold for all years using the ROLLUP function, allowing multiple grouping sets
-- Last view which SQL SELECT query will refer to.
CREATE VIEW orderView AS 
    SELECT yrUniqueView.actname, strYr, SUM(tktTrack) AS track FROM yrUniqueView INNER JOIN noOfTktsView ON noOfTktsView.actname = yrUniqueView.actname
    GROUP BY ROLLUP(yrUniqueView.actname, strYr), track;

-- used in OPTION 8
-- Takes cost of all tickets which has gigs that will go ahead and calculates average
--calls function requriredTkts() using venueid to identify homonymous venues
--- FULL OUTER JOIN means all records matching or not will be returned
-- AVG() function used to calcuate average value of cost column from ticket, and this value is casted to a INT
-- 
CREATE VIEW notOrdered AS 
    SELECT venueid, venuename, actname, requiredTkts(venueid, actname, 
    (SELECT AVG(cost) FROM ticket FULL OUTER JOIN gig ON gig.gigid = ticket.gigid WHERE gigstatus = 'GoingAhead')::BIGINT) as minTkts FROM act, venue;

-- used in OPTION 8
--calls function calculation() which will find the proportion value.
-- casts proportion to real. 
-- then order by alphabetical venue name, then proportion using function called, and then act name
CREATE VIEW ordered AS 
    SELECT venuename, actname, minTkts FROM notOrdered WHERE minTkts IS NOT NULL 
    ORDER BY venuename, calculation(venueid, minTkts::REAL) DESC, actname;



