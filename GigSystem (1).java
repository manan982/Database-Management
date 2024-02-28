import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;

import java.time.LocalDateTime;
import java.sql.Timestamp;
import java.util.Vector;
import java.util.jar.Attributes.Name;

import org.apache.commons.collections.functors.OnePredicate;
import org.apache.commons.lang3.text.translate.AggregateTranslator;


public class GigSystem {
    public static void main(String[] args) {

        // You should only need to fetch the connection details once
        // You might need to change this to either getSocketConnection() or getPortConnection() - see below
        Connection conn = getSocketConnection();

        boolean repeatMenu = true;
        
        while(repeatMenu){
            System.out.println("_________________________");
            System.out.println("________GigSystem________");
            System.out.println("_________________________");

            
            System.out.println("q: Quit");

            String menuChoice = readEntry("Please choose an option: ");

            if(menuChoice.length() == 0){
                //Nothing was typed (user just pressed enter) so start the loop again
                continue;
            }
            char option = menuChoice.charAt(0);

            /**
             * If you are going to implement a menu, you must read input before you call the actual methods
             * Do not read input from any of the actual option methods
             */

            //ption = readEntry(prompt);
            switch(option){
                case '1'
                    break;

                case '2':
                    break;
                case '3':
                    break;
                case '4':
                    break;
                case '5':
                    break;
                case '6':
                    break;
                case '7':
                    break;
                case '8':
                    break;
                case 'q':
                    repeatMenu = false;
                    break;
                default: 
                    System.out.println("Invalid option");
            }
        }
    }

    /*OPTION 1
    Takes in gigID as input, and finds acts taking place based on this gigID. 
    Returns a 2d array consisting of the name of the act, and times where the act starts at finishes
    */
 
    public static String[][] option1 (Connection conn, int gigID) {

        String[][] gigLineUp = {}; // creates an array that will store the line up and return it 


        // casts duration to a interval, which has the no. of minutes of each act
        // Add this to the current timestamp ontime, to get the end time that is the time after the interval has passed
        // cast the returned timestamp to a TIME variable
        String selectionQ = ("SELECT act.actname, ontime::TIME, (ontime + (duration::TEXT || 'minutes')::INTERVAL)::TIME " +
                                    "FROM act_gig JOIN act ON act_gig.actid = act.actid " +
                                    "WHERE act_gig.gigid = " + gigID + " ORDER BY ontime::TIME ASC;");
        
                                    // store SQL select query in string variable to be used in preparing a statement
                                
        try {   

            PreparedStatement statementPrepared  = conn.prepareStatement(selectionQ); 
            //use prepared statements to avoid SQL injections. SQL injections will use malicious code to access information not intended to be displayed.
            //also runs quicker than a normal query, as they are pre-compiled on database and has a definec access plan
            ResultSet rs = statementPrepared.executeQuery(); // will retrieve data from database by carrying out the sql query in the prepared statement object
            gigLineUp = convertResultToStrings(rs); 
            statementPrepared.close(); 
            rs.close();
            //close PreparedStatements and Resultsets to not exceed maximum open cursors, and for proper clean up
        } catch (SQLException e) { // makes sure database returns to previous state it was at before and cancels query.
            
            //catching the SQL exception produced by the prepareStatement object
            
            System.out.println("QUERY UNEXECUTABLE !"); 

            return null;
        }
        
        printTable(gigLineUp); //outputs results of query
        return gigLineUp;

    }


/* OPTION 2
Given a venue, allow a new gig to be set up, meaning data given about the gig is inserted into tables
Does not return anything
*/
    public static void option2(Connection conn, String venue, String gigTitle, int[] actIDs, int[] fees, LocalDateTime[] onTimes, int[] durations, int adultTicketPrice) {

    try {

        String initialisedStr = ""; //initialse empty string to store venueID
        int initialisedgigId = 0; //initialise gigId
        String initialisedPT = "A"; // set price type to be of type A to be used in the insert gig_tickrt query
        
        Timestamp TSgig = Timestamp.valueOf(onTimes[0]); // creating a object of Timestamp to store the casting of the localdatetime 

        PreparedStatement statementPreparedVenue = conn.prepareStatement("SELECT venueID FROM venue WHERE venuename = '" + venue +"'");
        // SQL select query straight into the prepared statement
        ResultSet rsVenue = statementPreparedVenue.executeQuery();
        
        if (rsVenue.next()) { // moving the position of the result set pointer to the next row
         initialisedStr = rsVenue.getString(1); //retrieves the value in the first column of the current row 
        }
        
        long venueID = Long.parseLong(initialisedStr); //resolves the venueID string into a signed decimal long 
        rsVenue.close();
        statementPreparedVenue.close();

        PreparedStatement statementPreparedGig = conn.prepareStatement("INSERT INTO gig (venueid, gigtitle, gigdate, gigstatus) VALUES (?,?,?,?)",PreparedStatement.RETURN_GENERATED_KEYS);
        //passing SQL insertion statement into a prepared statement object. Each ? will be populated with values before the query is executed, currently acting as a placeholder. 
        //Avoids need of preparing new statements every time to insert a new value, more efficient


        //setting the long/string/timestamp value to each parameter index ranging from 1-4
        statementPreparedGig.setLong(1,venueID); 
        statementPreparedGig.setString(2,gigTitle);
        statementPreparedGig.setTimestamp(3,TSgig); 
        statementPreparedGig.setString(4,"GoingAhead");
        statementPreparedGig.executeUpdate(); // instead of executeQuery() use executeUpdate() as outputting no results
        ResultSet rsGig = statementPreparedGig.getGeneratedKeys();
    
         if (rsGig.next()) {
             initialisedgigId = rsGig.getInt(1);
         }
         rsGig.close();
         statementPreparedGig.close();
        
        //Can use for loop, as the size of actIds is also the size of fees, durations and onTimes. Outer loop will ensure all values are inserted in array
         for (int x = 0; x < actIDs.length; x++) {
            Timestamp TSatTime = Timestamp.valueOf(onTimes[x]);  // casting of a timestamp object to localdatetime to allow SQL processing
            PreparedStatement statementPreparedAG = conn.prepareStatement("INSERT INTO act_gig (actID, gigID, actfee, ontime, duration) VALUES (?,?,?,?,?)");
             statementPreparedAG.setInt(1,actIDs[x]);
             statementPreparedAG.setInt(2,initialisedgigId);
             statementPreparedAG.setInt(3,fees[x]);
             statementPreparedAG.setTimestamp(4,TSatTime);
             statementPreparedAG.setInt(5,durations[x]);
             statementPreparedAG.executeUpdate();
             statementPreparedAG.close();
        }

        PreparedStatement statementPreparedTkt = conn.prepareStatement("INSERT INTO gig_ticket (gigID, pricetype, cost) VALUES (?,?,?)");
        statementPreparedTkt.setInt(1,initialisedgigId);
        statementPreparedTkt.setString(2,initialisedPT);
        statementPreparedTkt.setInt(3,adultTicketPrice);
        statementPreparedTkt.executeUpdate();
        statementPreparedTkt.close();
        
        } catch (SQLException e) { 
            System.out.println("QUERY UNEXECUTABLE!");
        }
}

/* OPTION 3
Allows customer to purchase a ticket. 
Price of the ticket is obtained from the gig_ticket table, corresponding to the inputted gigid
*/
    public static void option3 ( Connection conn, int gigid, String name, String email, String ticketType) {
        
        
        String insertTktQ = "INSERT INTO ticket VALUES (DEFAULT, ?, ?, (SELECT cost FROM gig_ticket WHERE gig_ticket.gigid = ?), ?, ?)";
        // SQL insert statement is the query passed into prepare statement object
        //can use default due to gigid being SERIAL, implying auto incrementation
        try {

            PreparedStatement statementPrepared = conn.prepareStatement(insertTktQ); 
            statementPrepared.setInt(1, gigid);
            statementPrepared.setString(2, ticketType); 
            statementPrepared.setInt(3, gigid);
            statementPrepared.setString(4, name);
            statementPrepared.setString(5, email);
            statementPrepared.executeUpdate(); 
            statementPrepared.close();
        } catch (SQLException e) { 
            System.out.println("QUERY UNEXECUTABLE!");
        }
    }


/* OPTION 4
Depending on entered gigid and actname, removes act from act_gig
If the act removal was a headline act, meaning the only act or the final, cancels the entire gig
Finds list of emails with customers affected in an array of strings
Otherwise, return null if gig does not require cancellation
*/
    public static String[] option4(Connection conn, int gigID, String actName) {
   
      try {

        boolean verifyHeadline = false; // initially declare act to not to be a headline for the given gig 
        int initalisedActID = 0; // set ID to be 0 intially
        
        // Will call function in SQL file that determines if the actName is a headline of provided gig according to gigID
        String selectionQHeadLine = "SELECT hlCheckerOpt4(?,?)"; 

        PreparedStatement statementPreparedHeadLine = conn.prepareStatement(selectionQHeadLine);
        statementPreparedHeadLine.setInt(1, gigID);
        statementPreparedHeadLine.setString(2, actName);
        ResultSet rsHeadline = statementPreparedHeadLine.executeQuery();

        while(rsHeadline.next()) {
            verifyHeadline = rsHeadline.getBoolean(1); // retrieve boolean true or false value whether act is a headline or not
        }

        String selectionQactID = "SELECT actID FROM act WHERE actname = ?"; 
        PreparedStatement statementPreparedactID = conn.prepareStatement(selectionQactID);
        // Select query in the prepared statement object that will find the according actID corresponding to the actname
        statementPreparedactID.setString(1, actName);
        ResultSet rsActID = statementPreparedactID.executeQuery();
        while(rsActID.next()) {
            initalisedActID = rsActID.getInt(1); //find integer actID of the act that shall be cancelled
        }

        PreparedStatement statementPreparedDeletion = conn.prepareStatement("DELETE FROM act_gig WHERE actid = ?");
        // from the act_gig table, removal of the act according to actid, which we found from inputted actname
        statementPreparedDeletion.setInt(1, initalisedActID);
        statementPreparedDeletion.executeUpdate(); 

        if (verifyHeadline = true) { // if act is headline, then carry out 

            ArrayList<String> affectedEmAddr = new ArrayList<>(); // creating arraylist to store emails of customers affected by cancellation
            String[] checkEmAddr = new String[affectedEmAddr.size()];

            // cancel the entire gig, set status to cancelled, coressponding to gigID
            PreparedStatement statementPreparedGStatus = conn.prepareStatement("UPDATE gig SET gigstatus = 'Cancelled' WHERE gigid = ?");
            statementPreparedGStatus.setInt(1, gigID);
            statementPreparedGStatus.executeUpdate();

            // use distinct in selection Query to ensure no duplicates.
            // finding all emails of those customers who have been affected, and order them.
            String selecionQunique = "SELECT DISTINCT customerEmail FROM ticket WHERE gigID = ? ORDER BY customerEmail";
            PreparedStatement statementPreparedEmail = conn.prepareStatement(selecionQunique);
            statementPreparedEmail.setInt(1, gigID);
            ResultSet rsEmails = statementPreparedEmail.executeQuery(); // Query rather than update as results, list of emails, are being returned

            while (rsEmails.next()){
                affectedEmAddr.add(rsEmails.getString(1)); // adding emails to ArrayList
            }
            
            //converting arraylist to a array of strings which can be returned 
            for (int x = 0;  x < affectedEmAddr.size(); x++) {
                checkEmAddr[x] = affectedEmAddr.get(x);
            }

            return checkEmAddr;

            
            }
    
    } catch (SQLException e) { 
        System.out.println("QUERY UNEXECUTABLE !");
        return null;
    }

    return null; // if gig does not require cancellation due to act being a headline act.

}

/*OPTION 5
Uses function that takes gigId as input to call function in schema.sql
function will calculate the tickets that still need to be sold in order to pay off agreed fees, of option type
*/

    public static String[][] option5 (Connection conn) {
        String selectionQ = "SELECT gigid, reqStantardTkts(gigid) FROM gig ORDER BY gigid ASC;";
        // SQL selection query to be passed into a PreparedStatement object. ordered by ascending value of gigid. 
        //Calls function from SQL file
        String[][] requiredTkts = {}; // array to store the tickets needed to be sold for each gig
       
        try {   

            PreparedStatement statementPrepared = conn.prepareStatement(selectionQ);
            ResultSet lineUpRS = statementPrepared.executeQuery();
            
            requiredTkts = convertResultToStrings(lineUpRS); // converting output of query into strings so can be stored in array
            lineUpRS.close();
            statementPrepared.close();
            
        } catch (SQLException e) { 
            System.out.println("QUERY UNEXECUTABLE !");
            return null;
        }

        printTable(requiredTkts);
        return requiredTkts; // returned two dimensional array of strings which has number of tickets needed to be sold along with gigid
        
    }

/*OPTION 6
Via functions and views specified in schema.sql, find the total tickets sold for certain acts annually, and total amount for all years combined
These certain acts will only be considered iff it is a headline act in a gig, and the actual gig cannot be cancelled
*/
    public static String[][] option6 (Connection conn) {
        
        String selectionQ = "SELECT actname, CASE WHEN strYr IS NULL THEN 'Total' ELSE strYr END, track FROM orderView WHERE actname IS NOT NULL"; 
        //CASE SQL selection statement that will go through conditions, will stop and return value when first condition is met; if there is no year then return total. Else, output the ticket track value from orderview in schema.sql
        //Will take the actname mentioned, along with the year as a string and total tickets from the ultimate view in the sql file
        //WHERE used to filter actnames which would appear as null in actname column, and remove these rows
        //Where the year column would be displayed as NULL for that row, as finding total tickets ever sold, change to Total
        String[][] allTkts = {};


        // Same structure as option 5. Passing query into PreparedStatement object, obtaining output of this query, and converting to allow storing as an array of strings
        try {

            PreparedStatement statementPrepared = conn.prepareStatement(selectionQ);
            ResultSet allTktsRs = statementPrepared.executeQuery();
            allTkts = convertResultToStrings(allTktsRs);
            allTktsRs.close();
            statementPrepared.close();
           
        } catch (SQLException e) { 
            System.out.println("QUERY UNEXECUTABLE !");
            return null;
        }

        printTable(allTkts);
        return allTkts; 
        //returns two dimensional array of strings, 
        //only considering gigs where act is a headliner, not only output tickets sold annually for all yrs act has run, but also the total of these tickets
    }

    public static String[][] option7 (Connection conn) {
        
        return null;
    }

    /* OPTION 8
    Finding the acts for each venue that would be economically feasible to book. 
    If economically feasible, return proportion of tickets needed to achieve this. Calculate proportion using function in SQL file
    Economically feasible means they can sell enough tickets to afford hiring the venue and acts standard fee.

*/

    public static String[][] option8 (Connection conn) {
        
        String selectionQ = "SELECT * FROM ordered";
        //SQL selection statement uses * meaning selects all data from the view ordered which is defined in schema.sql, which has already found all economically feasible gigs
        String[][] allGigs = {};

        // Same logic as option 5 and 6 used.
        try {

            PreparedStatement statementPrepared = conn.prepareStatement(selectionQ);
            ResultSet feasibleRs = statementPrepared.executeQuery();
            allGigs = convertResultToStrings(feasibleRs);
            feasibleRs.close();
            statementPrepared.close();
            
        } catch (SQLException e) { 
            System.out.println("QUERY UNEXECUTABLE !");
            return null;
        }


        printTable(allGigs);
        return allGigs;

        //returns two dimensional array of strings, 
        //which includes venues and respective acts that would provide an economically feasible booking, and proportion of tickets needed to be sold
        //Ordered by venue name alphabetically, then highest proportion, then alphabetical act name
    }


    

    /**
     * Prompts the user for input
     * @param prompt Prompt for user input
     * @return the text the user typed
     */

    private static String readEntry(String prompt) {
        
        try {
            StringBuffer buffer = new StringBuffer();
            System.out.print(prompt);
            System.out.flush();
            int c = System.in.read();
            while(c != '\n' && c != -1) {
                buffer.append((char)c);
                c = System.in.read();
            }
            return buffer.toString().trim();
        } catch (IOException e) {
            return "";
        }

    }
     
    /**
    * Gets the connection to the database using the Postgres driver, connecting via unix sockets
    * @return A JDBC Connection object
    */
    public static Connection getSocketConnection(){
        Properties props = new Properties();
        props.setProperty("socketFactory", "org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg");
        props.setProperty("socketFactoryArg",System.getenv("HOME") + "/cs258-postgres/postgres/tmp/.s.PGSQL.5432");
        Connection conn;
        try{
          conn = DriverManager.getConnection("jdbc:postgresql://localhost/cwk", props);
          return conn;
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Gets the connection to the database using the Postgres driver, connecting via TCP/IP port
     * @return A JDBC Connection object
     */
    public static Connection getPortConnection() {
        
        String user = "postgres";
        String passwrd = "password";
        Connection conn;

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException x) {
            System.out.println("Driver could not be loaded");
        }

        try {
            conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/cwk?user="+ user +"&password=" + passwrd);
            return conn;
        } catch(SQLException e) {
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
            System.out.println("Error retrieving connection");
            return null;
        }
    }

    public static String[][] convertResultToStrings(ResultSet rs) {
        Vector<String[]> output = null;
        String[][] out = null;
        try {
            int columns = rs.getMetaData().getColumnCount();
            output = new Vector<String[]>();
            int rows = 0;
            while(rs.next()){
                String[] thisRow = new String[columns];
                for(int i = 0; i < columns; i++){
                    thisRow[i] = rs.getString(i+1);
                }
                output.add(thisRow);
                rows++;
            }
            // System.out.println(rows + " rows and " + columns + " columns");
            out = new String[rows][columns];
            for(int i = 0; i < rows; i++){
                out[i] = output.get(i);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return out;
    }

    public static void printTable(String[][] out) {
        int numCols = out[0].length;
        int w = 20;
        int widths[] = new int[numCols];
        for(int i = 0; i < numCols; i++){
            widths[i] = w;
        }
        printTable(out,widths);
    }

    public static void printTable(String[][] out, int[] widths) {
        for(int i = 0; i < out.length; i++){
            for(int j = 0; j < out[i].length; j++){
                System.out.format("%"+widths[j]+"s",out[i][j]);
                if(j < out[i].length - 1){
                    System.out.print(",");
                }
            }
            System.out.println();
        }
    }

}
