import psycopg2
import pandas as pd

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname="PMS",
            user="postgres",
            password="july@2025",
            host="localhost",
            port="5432"
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error connecting to database: {e}")
        return None

# --- EMPLOYEE CRUD ---
def get_all_employees():
    """Reads all employees from the database."""
    conn = get_db_connection()
    if conn:
        with conn.cursor() as cur:
            cur.execute("SELECT employee_id, first_name, last_name, role FROM employees")
            employees = cur.fetchall()
            return employees
    return []

# --- GOAL CRUD ---
def create_goal(employee_id, description, due_date, created_by):
    """Creates a new goal for an employee."""
    conn = get_db_connection()
    if conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO goals (employee_id, description, due_date, created_by) VALUES (%s, %s, %s, %s) RETURNING goal_id",
                (employee_id, description, due_date, created_by)
            )
            goal_id = cur.fetchone()[0]
            conn.commit()
            return goal_id

def get_goals_for_employee(employee_id):
    """Reads all goals for a specific employee."""
    conn = get_db_connection()
    if conn:
        query = """
            SELECT goal_id, description, due_date, status
            FROM goals
            WHERE employee_id = %s
            ORDER BY due_date DESC
        """
        return pd.read_sql_query(query, conn, params=(employee_id,))
    return pd.DataFrame()

def update_goal_status(goal_id, new_status):
    """Updates the status of a specific goal."""
    conn = get_db_connection()
    if conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE goals SET status = %s WHERE goal_id = %s",
                (new_status, goal_id)
            )
            conn.commit()

# --- TASK CRUD ---
def create_task(goal_id, description, created_by):
    """Creates a new task linked to a goal."""
    conn = get_db_connection()
    if conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO tasks (goal_id, description, created_by) VALUES (%s, %s, %s) RETURNING task_id",
                (goal_id, description, created_by)
            )
            task_id = cur.fetchone()[0]
            conn.commit()
            return task_id

def get_tasks_for_goal(goal_id):
    """Reads all tasks for a specific goal."""
    conn = get_db_connection()
    if conn:
        query = """
            SELECT task_id, description, status
            FROM tasks
            WHERE goal_id = %s
        """
        return pd.read_sql_query(query, conn, params=(goal_id,))
    return pd.DataFrame()

def update_task_status(task_id, new_status):
    """Updates the status of a specific task."""
    conn = get_db_connection()
    if conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE tasks SET status = %s WHERE task_id = %s",
                (new_status, task_id)
            )
            conn.commit()

# --- FEEDBACK CRUD ---
def create_feedback(goal_id, employee_id, manager_id, feedback_text):
    """Adds feedback for an employee on a specific goal."""
    conn = get_db_connection()
    if conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO feedback (goal_id, employee_id, manager_id, feedback_text) VALUES (%s, %s, %s, %s)",
                (goal_id, employee_id, manager_id, feedback_text)
            )
            conn.commit()

def get_feedback_for_employee(employee_id):
    """Reads all feedback for a specific employee."""
    conn = get_db_connection()
    if conn:
        query = """
            SELECT f.feedback_text, e_m.first_name, e_m.last_name, g.description, f.created_at
            FROM feedback f
            JOIN employees e_m ON f.manager_id = e_m.employee_id
            JOIN goals g ON f.goal_id = g.goal_id
            WHERE f.employee_id = %s
            ORDER BY f.created_at DESC
        """
        return pd.read_sql_query(query, conn, params=(employee_id,))
    return pd.DataFrame()

# --- REPORTING & ANALYTICS ---
def get_insights():
    """Fetches key business insights using SQL aggregate functions."""
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM goals")
                total_goals = cur.fetchone()[0]

                cur.execute("SELECT COUNT(DISTINCT employee_id) FROM employees WHERE role = 'Employee'")
                total_employees = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM tasks WHERE status = 'Approved'")
                approved_tasks = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM tasks WHERE status = 'Pending'")
                pending_tasks = cur.fetchone()[0]

                cur.execute("SELECT AVG(goal_count) FROM (SELECT COUNT(*) AS goal_count FROM goals GROUP BY employee_id) AS subquery")
                avg_goals_per_employee = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM goals WHERE status = 'Completed'")
                completed_goals = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM goals WHERE status = 'In Progress'")
                in_progress_goals = cur.fetchone()[0]

                return {
                    "total_goals": total_goals,
                    "avg_goals_per_employee": f"{avg_goals_per_employee:.2f}" if avg_goals_per_employee else 0,
                    "approved_tasks": approved_tasks,
                    "pending_tasks": pending_tasks,
                    "completed_goals": completed_goals,
                    "in_progress_goals": in_progress_goals,
                }
        except (Exception, psycopg2.Error) as error:
            print(f"Error fetching insights: {error}")
            return {}
    return {}
