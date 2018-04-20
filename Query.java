import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.io.FileInputStream;

/**
 * Runs queries against a back-end database
 */
public class Query {
	private static Properties configProps = new Properties();

	private static String MySqlServerDriver;
	private static String MySqlServerUrl;
    private static String MySqlServerUser;
	private static String MySqlServerPassword;

	private static String PsotgreSqlServerDriver;
	private static String PostgreSqlServerUrl;
	private static String PostgreSqlServerUser;
	private static String PostgreSqlServerPassword;


	// DB Connection
	private Connection _mySqlDB; //IMDB
    private Connection _postgreSqlDB; //customer_DB

	// Canned queries

	private String _search_sql = "SELECT * FROM movie_info WHERE movie_name like ? ORDER BY movie_id";
	private PreparedStatement _search_statement;

	private String _producer_id_sql = "SELECT y.* "
					 + "FROM producer_movies x, producer_ids y "
					 + "WHERE x.movie_id = ? and x.producer_id = y.producer_id";
	private PreparedStatement _producer_id_statement;

	/*Q1.*/
	////////////////////////////////
	private String _actor_id_sql = "SELECT a.* "
						+ "FROM actor_ids a, actor_movies y "
						+ "WHERE y.movie_id = ? and a.actor_id = y.actor_id";
	private PreparedStatement _actor_id_statement;

	private String _movie_status_sql = "SELECT cid FROM myrental WHERE mid = ? AND status='open'";
	private PreparedStatement _movie_status_statement;

	////////////////////////////////

	 //uncomment, and edit, after your create your own customer database
	private String _customer_login_sql = "SELECT * FROM mycustomer WHERE login = ? and password = ?";
	private PreparedStatement _customer_login_statement;

	/*Q2*/
	////////////////////////////////
	private String _plan_list_sql = "SELECT * FROM myplan";
	private PreparedStatement _plan_list_statement;

	private String _update_plan_sql = "UPDATE mycustomer SET rental_plan = ? where cid = ?";
	private PreparedStatement _update_plan_statement;

	private String _search_plan_sql = "SELECT * FROM myplan WHERE pid = ?";
	private PreparedStatement _search_plan_statement;
	////////////////////////////////

	/*Q3*/
	////////////////////////////////
	private String _valid_movieid_sql = "SELECT * FROM movie_info WHERE movie_id = ?";
	private PreparedStatement _valid_movieid_statement;

	private String _get_maxnum_sql = "SELECT max_num FROM myplan p JOIN mycustomer c ON c.rental_plan = p.pid WHERE c.cid = ?";
	private PreparedStatement _get_maxnum_statement;

	private String _current_rent_num_sql = "SELECT count(*) FROM myrental WHERE cid= ? AND status='open'";
	private PreparedStatement _current_rent_num_statement;

	private String _check_times_sql = "SELECT max(rental_time) FROM myrental WHERE cid = ? AND mid = ? AND status='closed'";
	private PreparedStatement _check_times_statement;

	private String _insert_customer_rental_sql = "INSERT INTO myrental (cid, mid, status, rental_time) values (?, ?, 'open', ?);";
	private PreparedStatement _insert_customer_rental_statement;
	////////////////////////////////

	/*Q4.*/
	////////////////////////////////
	private String _return_movie_sql = "UPDATE myrental SET status='closed' WHERE cid = ? AND mid = ?";
	private PreparedStatement _return_movie_statement;
	////////////////////////////////

	/*Q5.*/
	////////////////////////////////
	private String _search_producer_sql = "SELECT x.movie_id,y.producer_name FROM producer_movies x JOIN producer_ids y "+
					"ON x.producer_id=y.producer_id JOIN movie_info z ON z.movie_id=x.movie_id "+
					"WHERE z.movie_name like ? ORDER BY x.movie_id";
	private PreparedStatement _search_producer_statement;

	private String _search_actor_sql = "SELECT x.movie_id,y.actor_name FROM actor_movies x JOIN actor_ids y "+
					"ON x.actor_id=y.actor_id JOIN movie_info z ON z.movie_id=x.movie_id "+
					"WHERE movie_name like ? ORDER BY x.movie_id";
	private PreparedStatement _search_actor_statement;
	////////////////////////////////

	private String _personal_name_sql = "SELECT concat(x.first_name,' ', x.last_name) FROM mycustomer x WHERE x.cid= ?";
	private PreparedStatement _personal_name_statement;

	private String _begin_transaction_read_write_sql = "START TRANSACTION";
	private PreparedStatement _begin_transaction_read_write_statement;

	private String _commit_transaction_sql = "COMMIT";
	private PreparedStatement _commit_transaction_statement;

	private String _rollback_transaction_sql = "ROLLBACK";
	private PreparedStatement _rollback_transaction_statement;


	public Query() {
	}

    /**********************************************************/
    /* Connection to MySQL database */

	public void openConnections() throws Exception {

        /* open connections to TWO databases: movie and  customer databases */

		configProps.load(new FileInputStream("dbconn.config"));

		MySqlServerDriver    = configProps.getProperty("MySqlServerDriver");
		MySqlServerUrl 	   = configProps.getProperty("MySqlServerUrl");
		MySqlServerUser 	   = configProps.getProperty("MySqlServerUser");
		MySqlServerPassword  = configProps.getProperty("MySqlServerPassword");

        PsotgreSqlServerDriver    = configProps.getProperty("PostgreSqlServerDriver");
        PostgreSqlServerUrl 	   = configProps.getProperty("PostgreSqlServerUrl");
        PostgreSqlServerUser 	   = configProps.getProperty("PostgreSqlServerUser");
        PostgreSqlServerPassword  = configProps.getProperty("PostgreSqlServerPassword");

		/* load jdbc driver for MySQL */
		Class.forName(MySqlServerDriver).newInstance();

		/* open a connection to your mySQL database that contains the movie database */
		_mySqlDB = DriverManager.getConnection(MySqlServerUrl, // database
				MySqlServerUser, // user
				MySqlServerPassword); // password


        /* load jdbc driver for PostgreSQL */
        Class.forName(PsotgreSqlServerDriver).newInstance();

         /* connection string for PostgreSQL */
        String PostgreSqlConnectionString = PostgreSqlServerUrl+"?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&user="+
        		PostgreSqlServerUser+"&password=" + PostgreSqlServerPassword;


        /* open a connection to your postgreSQL database that contains the customer database */
        _postgreSqlDB = DriverManager.getConnection(PostgreSqlConnectionString);


	}

	public void closeConnections() throws Exception {
		_mySqlDB.close();
        _postgreSqlDB.close();
	}

    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

	public void prepareStatements() throws Exception {

		_search_statement = _mySqlDB.prepareStatement(_search_sql);
		_producer_id_statement = _mySqlDB.prepareStatement(_producer_id_sql);
		/*Q1.*/
		////////////////////////////////
		_actor_id_statement = _mySqlDB.prepareStatement(_actor_id_sql);
		_movie_status_statement = _postgreSqlDB.prepareStatement(_movie_status_sql);
		////////////////////////////////
		/* uncomment after you create your customers database */

		/*Q2.*/
		////////////////////////////////
		_plan_list_statement = _postgreSqlDB.prepareStatement(_plan_list_sql);
		_update_plan_statement = _postgreSqlDB.prepareStatement(_update_plan_sql);
		_search_plan_statement = _postgreSqlDB.prepareStatement(_search_plan_sql);
		////////////////////////////////

		/*Q3.*/
		////////////////////////////////
		_valid_movieid_statement = _mySqlDB.prepareStatement(_valid_movieid_sql);
		_get_maxnum_statement = _postgreSqlDB.prepareStatement(_get_maxnum_sql);
		_current_rent_num_statement = _postgreSqlDB.prepareStatement(_current_rent_num_sql);
		_check_times_statement = _postgreSqlDB.prepareStatement(_check_times_sql);
		_insert_customer_rental_statement = _postgreSqlDB.prepareStatement(_insert_customer_rental_sql);
		////////////////////////////////

		/*Q4.*/
		////////////////////////////////
		_return_movie_statement = _postgreSqlDB.prepareStatement(_return_movie_sql);
		////////////////////////////////

		/*Q5.*/
		////////////////////////////////
		_search_producer_statement = _mySqlDB.prepareStatement(_search_producer_sql);
		_search_actor_statement = _mySqlDB.prepareStatement(_search_actor_sql);
		////////////////////////////////

		_personal_name_statement = _postgreSqlDB.prepareStatement(_personal_name_sql);

		_customer_login_statement = _postgreSqlDB.prepareStatement(_customer_login_sql);
		_begin_transaction_read_write_statement = _postgreSqlDB.prepareStatement(_begin_transaction_read_write_sql);
		_commit_transaction_statement = _postgreSqlDB.prepareStatement(_commit_transaction_sql);
		_rollback_transaction_statement = _postgreSqlDB.prepareStatement(_rollback_transaction_sql);


		/* add here more prepare statements for all the other queries you need */
		/* . . . . . . */
	}


    /**********************************************************/
    /* suggested helper functions  */
	public int helper_check_times(int cid, String movie_id) throws Exception {
			/*Q3.*/
			int times = 0;
			_check_times_statement.clearParameters();
			_check_times_statement.setInt(1, cid);
			_check_times_statement.setString(2, movie_id);
			ResultSet check_times_set = _check_times_statement.executeQuery();
			while (check_times_set.next()) {
					times = check_times_set.getInt(1);
			}

			return times;
	}

	public int helper_compute_remaining_rentals(int cid) throws Exception {
		/* how many movies can she/he still rent ? */
		/* you have to compute and return the difference between the customer's plan
		   and the count of outstanding rentals */
		/*Q3.*/
		int current_rentnum = 0;
		int max_num = 0;
		_get_maxnum_statement.clearParameters();
		_get_maxnum_statement.setInt(1, cid);
		ResultSet maxnum_set = _get_maxnum_statement.executeQuery();
		while (maxnum_set.next()) {
				max_num = maxnum_set.getInt(1);
		}

		_current_rent_num_statement.clearParameters();
		_current_rent_num_statement.setInt(1, cid);
		ResultSet current_rent_num = _current_rent_num_statement.executeQuery();
		while (current_rent_num.next()) {
				current_rentnum = current_rent_num.getInt(1);
		}

		return max_num - current_rentnum;
	}

	public String helper_compute_customer_name(int cid) throws Exception {
		/* you find  the name of the current customer */
		//return ("Joe Name");
		_personal_name_statement.clearParameters();
		_personal_name_statement.setInt(1, cid);
		ResultSet personal_name_set = _personal_name_statement.executeQuery();
		if (personal_name_set.next()){
						return personal_name_set.getString(1);
		}
		return ("NOT Found");

	}

	public boolean helper_check_plan(int plan_id) throws Exception {
		/* is plan_id a valid plan id?  you have to figure out */
		/*Q2.*/
		boolean status = false;
		_search_plan_statement.clearParameters();
		_search_plan_statement.setInt(1, plan_id);
		ResultSet search_plan_set = _search_plan_statement.executeQuery();
		while (search_plan_set.next()) {
			status = true;
		}
		return status;
	}

	public boolean helper_check_movie(String movie_id) throws Exception {
		/* is movie_id a valid movie id? you have to figure out  */
		/*Q3.*/
		boolean valid = false;
		_valid_movieid_statement.clearParameters();
		_valid_movieid_statement.setString(1, movie_id);
		ResultSet valid_movieid = _valid_movieid_statement.executeQuery();
		while (valid_movieid.next()) {
				valid = true;
		}
		return valid;
	}

	private int helper_who_has_this_movie(String movie_id) throws Exception {
		/* find the customer id (cid) of whoever currently rents the movie movie_id; return -1 if none */
		int customer_id = -1;
		int tmp_time = 0;
		String tmp_status = "closed";
		String status;
		int rental_time;
		_movie_status_statement.clearParameters();
		_movie_status_statement.setString(1, movie_id);
		ResultSet movie_status_set = _movie_status_statement.executeQuery();

		while (movie_status_set.next()) {
			customer_id = movie_status_set.getInt(1);
		}
		movie_status_set.close();
		return customer_id;
	}

    /**********************************************************/
    /* login transaction: invoked only once, when the app is started  */
	public int transaction_login(String name, String password) throws Exception {
		/* authenticates the user, and returns the user id, or -1 if authentication fails */

		/* Uncomment after you create your own customers database */

		int cid;

		_customer_login_statement.clearParameters();
		_customer_login_statement.setString(1,name);
		_customer_login_statement.setString(2,password);
	    ResultSet cid_set = _customer_login_statement.executeQuery();
	    if (cid_set.next()) cid = cid_set.getInt(1);
		else cid = -1;
		return(cid);

		//return (55); //comment after you create your own customers database
	}

	public void transaction_personal_data(int cid) throws Exception {
		/* println the customer's personal data: name and number of remaining rentals */
		System.out.println("Name: "+ helper_compute_customer_name(cid)+
								"\nAmount you can rent: " + helper_compute_remaining_rentals(cid));
	}


    /**********************************************************/
    /* main functions in this project: */

	public void transaction_search(int cid, String movie_name)
			throws Exception {
		/* searches for movies with matching names: SELECT * FROM movie WHERE movie_name LIKE name */
		/* prints the movies, producers, actors, and the availability status:
		   AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT */

		/* set the first (and single) '?' parameter */
		_search_statement.clearParameters();
		_search_statement.setString(1, '%' + movie_name + '%');

		ResultSet movie_set = _search_statement.executeQuery();
		while (movie_set.next()) {
			String movie_id = movie_set.getString(1);
			System.out.println("ID: " + movie_id + " NAME: "
					+ movie_set.getString(2) + " YEAR: "
					+ movie_set.getString(3) + " RATING: "
					+ movie_set.getString(4));
			/* do a dependent join with producer */
			_producer_id_statement.clearParameters();
			_producer_id_statement.setString(1, movie_id);
			ResultSet producer_set = _producer_id_statement.executeQuery();
			while (producer_set.next()) {
				System.out.println("\t\tProducer name: " + producer_set.getString(2));
			}
			producer_set.close();
			/* Q1  now you need to retrieve the actors, in the same manner */
			////////////////////////////////
			_actor_id_statement.clearParameters();
			_actor_id_statement.setString(1, movie_id);
			ResultSet actor_set = _actor_id_statement.executeQuery();
			while (actor_set.next()) {
				System.out.println("\t\t Actor name: " + actor_set.getString(2));
			}
			actor_set.close();

			/* then you have to find the status: of "AVAILABLE" "YOU HAVE IT", "UNAVAILABLE" */
			int customer_id = helper_who_has_this_movie(movie_id);
			if (customer_id == -1) {
				System.out.println("\t\t It is avalialbe.");
			} else if (customer_id == cid) {
				System.out.println("\t\t You already rented it.");
			} else {
				System.out.println("\t\t No avaliable now!");
			}
			////////////////////////////////
		}
		System.out.println();
	}

	public void transaction_choose_plan(int cid, int pid) throws Exception {
	    /* updates the customer's plan to pid: UPDATE customer SET plid = pid */
			/*Q2.*/
			if (helper_check_plan(pid) != true) {
					System.out.println("invalid plan id");
					return;
			}
			_update_plan_statement.clearParameters();
			_update_plan_statement.setInt(1, pid);
			_update_plan_statement.setInt(2, cid);
			_begin_transaction_read_write_statement.executeUpdate();
			int update_status = _update_plan_statement.executeUpdate();
			if (update_status != 0) {
					System.out.println("You just change your plan");
					_commit_transaction_statement.executeUpdate();
			} else {
					System.out.println("Change failed!");
			}
	    /* remember to enforce consistency ! */
	}

	public void transaction_list_plans() throws Exception {
	    /* println all available plans: SELECT * FROM plan */
			/*Q2.*/
			ResultSet plan_list_set = _plan_list_statement.executeQuery();
			while (plan_list_set.next()) {
					System.out.println("\t\t PLAN NAME: " + plan_list_set.getString(2) +
						" MAXIMUM NUMBER: " + plan_list_set.getInt(3) +
						" MONTHLY FEE: " + plan_list_set.getInt(4));
			}
			plan_list_set.close();
	}

	public void transaction_rent(int cid, String movie_id) throws Exception {
	    /* rend the movie movie_id to the customer cid */
	    /* remember to enforce consistency ! */
			if (helper_compute_remaining_rentals(cid) <= 0) {
					System.out.println("\t\t You already rent the maximum of your plan. Deny your action!");
			}

			if (helper_check_movie(movie_id) != true) {
					System.out.println("\t\t Invalid movie id!");
					return;
			}

			if (helper_who_has_this_movie(movie_id) != -1) {
					System.out.println("\t\t The movie is not avaliable!");
					return;
			}

			int times = helper_check_times(cid, movie_id);

			_insert_customer_rental_statement.clearParameters();
			_insert_customer_rental_statement.setInt(1, cid);
			_insert_customer_rental_statement.setString(2, movie_id);
			_insert_customer_rental_statement.setInt(3, times + 1);
			_begin_transaction_read_write_statement.executeUpdate();
			int update_status = _insert_customer_rental_statement.executeUpdate();
			if (update_status != 0) {
					System.out.println("\t\t Rent success ");
					_commit_transaction_statement.executeUpdate();
			} else {
					System.out.println("\t\t Rent failed!");
			}
	}

	public void transaction_return(int cid, String movie_id) throws Exception {
	    /* return the movie_id by the customer cid */
			//int rental_time = helper_check_times(cid, movie_id);
			_return_movie_statement.clearParameters();
			_return_movie_statement.setInt(1, cid);
			_return_movie_statement.setString(2, movie_id);
			//_return_movie_statement.setInt(3, rental_time);
			if (helper_who_has_this_movie(movie_id) == cid) {

					_begin_transaction_read_write_statement.executeUpdate();
					int update_status = _return_movie_statement.executeUpdate();
					if (update_status != 0) {
						System.out.println("\t\t Return success");
						_commit_transaction_statement.executeUpdate();
					} else {
						System.out.println("\t\t Return failed");
					}
			} else {
					System.out.println("\t\t You cannot return the movie you do not have.");
			}
	}

	public void transaction_fast_search(int cid, String movie_name)
			throws Exception {
		/* like transaction_search, but uses joins instead of dependent joins
		   Needs to run three SQL queries: (a) movies, (b) movies join producers, (c) movies join actors
		   Answers are sorted by movie_id.
		   Then merge-joins the three answer sets */

			 _search_statement.clearParameters();
			 _search_statement.setString(1, '%' + movie_name + '%');
			 ResultSet movie_set = _search_statement.executeQuery();

			 _search_producer_statement.clearParameters();
			 _search_producer_statement.setString(1, '%' + movie_name + '%');
			 ResultSet producer_set = _search_producer_statement.executeQuery();

			 _search_actor_statement.clearParameters();
			 _search_actor_statement.setString(1, '%' + movie_name + '%');
			 ResultSet actor_set = _search_actor_statement.executeQuery();

			 while (movie_set.next()) {
				 		String movie_id = movie_set.getString(1);
						System.out.println("ID: " + movie_id + " NAME: "
								+ movie_set.getString(2) + " YEAR: "
								+ movie_set.getString(3) + " RATING: "
								+ movie_set.getString(4));

						while (producer_set.next()) {
								if (producer_set.getString(1).equals(movie_id)) {
										System.out.println("\t\tProducer name : " + producer_set.getString(2));
								} else {
										break;
								}
						}
						producer_set.previous();

						while (actor_set.next()) {
								if (actor_set.getString(1).equals(movie_id)) {
										System.out.println("\t\t Actor name: " + actor_set.getString(2));
								} else {
									break;
								}

						}
						actor_set.previous();

			 }

	}

}
