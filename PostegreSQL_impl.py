import psycopg2

# Method to create tables in our PostgreSQL database. In the command we create the table named "employees"
# with the employee_id as the primary key and two columns for employee_name and employee_salary
def create_tables():
    command = ("""CREATE TABLE employees(
                employee_id SERIAL PRIMARY KEY,
                employee_name VARCHAR(255) NOT NULL,
                employee_salary INTEGER NOT NULL)""""")
    conn = None
    try:
        conn = psycopg2.connect(host='pg-287bf68e-konstatheodoridi-30f9.aivencloud.com:2745', database='defaultdb', user='avnadmin',
                                password='************')      # connecting to our database
        cursor = conn.cursor()
        cursor.execute(command)                         # we execute the command to create the specified table
        cursor.close()
        conn.commit()                                   # we commit tha changes
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)                                    # if there's an error then print it
    finally:
        if conn is not None:
            conn.close()                                # and close the connection
            print('Database connection closed.')


def insert_data(empl_name, empl_salary):
    sql = "INSERT INTO employees(employee_name, employee_salary) VALUES(%s, %s)"
    conn = None
    try:
        conn = psycopg2.connect(host='pg-287bf68e-konstatheodoridi-30f9.aivencloud.com:2745', database='defaultdb', user='avnadmin',
                                password='*************')  # connecting to our database
        cursor = conn.cursor()
        cursor.execute(sql, (empl_name, empl_salary))
        conn.commit()
        cursor.close()
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')