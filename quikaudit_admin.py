import streamlit as st
from pymongo import MongoClient
import pymysql
from datetime import datetime
import pandas as pd

# ─── CONFIG ───────────────────────────────────────────────────────────────────

MONGODB_URI = st.secrets["MONGODB_URI"]
MARIADB_URI = st.secrets["MARIADB_URI"]
APP_PASSWORD = st.secrets["APP_PASSWORD"]

@st.cache_data(ttl=300)
def load_organizations():
    uri = st.secrets["MARIADB_URI"].replace("mysql+pymysql://", "")
    user_pass, rest = uri.split("@")
    user, password = user_pass.split(":")
    host_db = rest.split("/")
    host = host_db[0]
    db = host_db[1]
    conn = pymysql.connect(host=host, user=user, password=password, database=db, cursorclass=pymysql.cursors.DictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT org_id, name FROM organizations ORDER BY name")
            rows = cur.fetchall()
        return {r["name"]: r["org_id"] for r in rows}
    finally:
        conn.close()

ORGANIZATIONS = load_organizations()

COLLECTIONS = {
    "vendors":      {"field": "vendor",     "label": "Vendor Name",     "extra_fields": [{"key": "initials", "label": "Initials"}]},
    "colors":       {"field": "color",      "label": "Color Name",      "extra_fields": []},
    "fabric_types": {"field": "fabric_type","label": "Fabric Type",     "extra_fields": []},
    "parties":      {"field": "party",      "label": "Party Name",      "extra_fields": [{"key": "initials", "label": "Initials"}]},
    "job_workers":  {"field": "job_worker", "label": "Job Worker Name", "extra_fields": []},
    "merchants":    {"field": "merchant",   "label": "Merchant Name",   "extra_fields": []},
    "plans":        {"field": "plan",       "label": "Plan",            "extra_fields": []},
    "receivers":    {"field": "name",       "label": "Receiver Name",   "extra_fields": [{"key": "type", "label": "Type", "default": "printer"}]},
    "designs":      {"field": "design_no",  "label": "Design Number",   "extra_fields": []},
}

JOB_WORKER_TYPES = {"Outsource": 1, "In_House": 2}
JOB_WORKER_ROLES = {"Cutting": 1, "Printing": 2, "Embroidery": 3, "Stitching": 4, "Checking": 5, "Ironing": 6, "Packing": 7}
ADDRESS_TYPES = {"OFFICE": 1, "LOCAL": 2}
PHONE_TYPE_ID = 1
EMAIL_TYPE_ID = 1

# ─── DB CONNECTIONS ───────────────────────────────────────────────────────────

@st.cache_resource
def get_audit_db():
    client = MongoClient(MONGODB_URI)
    return client["audit_db"]["audit_collection_new"]

@st.cache_resource
def get_mongo_db():
    client = MongoClient(MONGODB_URI)
    return client["organization_db"]



def get_mariadb_conn():
    uri = MARIADB_URI.replace("mysql+pymysql://", "")
    user_pass, rest = uri.split("@")
    user, password = user_pass.split(":")
    host_db = rest.split("/")
    host = host_db[0]
    db = host_db[1]
    return pymysql.connect(host=host, user=user, password=password, database=db, cursorclass=pymysql.cursors.DictCursor)

def run_query(sql, params=None, fetch=True):
    conn = get_mariadb_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            if fetch:
                return cur.fetchall()
            else:
                conn.commit()
                return cur.rowcount
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def run_many(sql, data_list):
    conn = get_mariadb_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, data_list)
            conn.commit()
            return cur.rowcount
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# ─── PAGE ─────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="QuikAudit Admin", page_icon="🧵", layout="wide")

# ─── AUTH ─────────────────────────────────────────────────────────────────────

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.title("🧵 QuikAudit Admin")
    st.markdown("Please enter the password to continue.")
    pwd = st.text_input("Password", type="password", key="login_pwd")
    if st.button("Login", type="primary"):
        if pwd == APP_PASSWORD:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    st.stop()

st.title("🧵 QuikAudit Admin Panel")
st.caption("Manage master data for Thrive Fashion and Caesar Industries")

# ─── SIDEBAR ──────────────────────────────────────────────────────────────────

st.sidebar.header("Select Organisation")
org_name = st.sidebar.selectbox("Organisation", list(ORGANIZATIONS.keys()))
org_id = ORGANIZATIONS[org_name]
st.sidebar.markdown("---")
st.sidebar.markdown(f"**Org ID:**\n`{org_id}`")
st.sidebar.markdown("---")
if st.sidebar.button("🚪 Sign Out"):
    st.session_state.authenticated = False
    st.rerun()

# ─── MAIN TABS ────────────────────────────────────────────────────────────────

main_tab1, main_tab2, main_tab3, main_tab4 = st.tabs(["📦 MongoDB Master Data", "👷 Job Workers (MariaDB)", "🛠️ Support Tools", "🧾 Purchase Management"])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1: MONGODB
# ═══════════════════════════════════════════════════════════════════════════════

with main_tab1:
    collection_name = st.selectbox("Collection", list(COLLECTIONS.keys()))
    col_config = COLLECTIONS[collection_name]
    main_field = col_config["field"]
    main_label = col_config["label"]
    extra_fields = col_config["extra_fields"]

    db = get_mongo_db()
    collection = db[collection_name]

    tab1, tab2, tab3 = st.tabs(["➕ Add Entry", "📋 View & Delete", "📥 Bulk Add"])

    with tab1:
        st.subheader(f"Add {main_label} for {org_name}")
        main_value = st.text_input(main_label, key="add_main")
        extra_values = {}
        for ef in extra_fields:
            extra_values[ef["key"]] = st.text_input(ef["label"], value=ef.get("default", ""), key=f"add_{ef['key']}")
        if st.button("Add Entry", type="primary", key="add_btn"):
            if not main_value.strip():
                st.error(f"{main_label} cannot be empty.")
            else:
                doc = {"organization_id": org_id, main_field: main_value.strip().lower()}
                for key, val in extra_values.items():
                    doc[key] = val.strip().lower() if val.strip() else ""
                collection.insert_one(doc)
                st.success(f"Added '{main_value}' to {collection_name} for {org_name}.")

    with tab2:
        st.subheader(f"All {collection_name} for {org_name}")
        docs = list(collection.find({"organization_id": org_id}, {"_id": 1, main_field: 1, **{ef["key"]: 1 for ef in extra_fields}}))
        if not docs:
            st.info("No entries found.")
        else:
            search = st.text_input("Search", placeholder=f"Filter by {main_label.lower()}...", key="search")
            filtered = [d for d in docs if not search or search.lower() in str(d.get(main_field, "")).lower()]
            st.caption(f"Showing {len(filtered)} of {len(docs)} entries")
            options = []
            id_map = {}
            for doc in filtered:
                display = doc.get(main_field, "")
                extras = " | ".join([f"{ef['key']}: {doc.get(ef['key'], '')}" for ef in extra_fields if doc.get(ef['key'])])
                label = f"{display}" + (f" [{extras}]" if extras else "")
                options.append(label)
                id_map[label] = doc["_id"]
            if options:
                selected = st.selectbox("Select entry to delete", options, key="delete_select")
                if st.button("Delete Selected", type="primary", key="delete_btn"):
                    collection.delete_one({"_id": id_map[selected]})
                    st.success(f"Deleted '{selected}'")
                    st.rerun()

    with tab3:
        st.subheader(f"Bulk Add {main_label} for {org_name}")
        if extra_fields:
            field_hint = ", ".join([ef["key"] for ef in extra_fields])
            st.info(f"Format: value,{field_hint} per line.")
        else:
            st.info(f"Paste one {main_label.lower()} per line.")
        bulk_text = st.text_area("Paste entries here", height=300, key="bulk_text")
        if st.button("Bulk Insert", type="primary", key="bulk_btn"):
            if not bulk_text.strip():
                st.error("Nothing to insert.")
            else:
                lines = [l.strip() for l in bulk_text.strip().splitlines() if l.strip()]
                existing_docs = collection.find({"organization_id": org_id}, {main_field: 1, "_id": 0})
                existing_values = set(d.get(main_field, "").lower() for d in existing_docs)
                docs_to_insert = []
                skipped = 0
                for line in lines:
                    parts = [p.strip() for p in line.split(",")]
                    main_val = parts[0].lower() if parts[0] else ""
                    if not main_val or main_val in existing_values:
                        skipped += 1
                        continue
                    doc = {"organization_id": org_id, main_field: main_val}
                    for i, ef in enumerate(extra_fields):
                        doc[ef["key"]] = parts[i+1].lower() if len(parts) > i+1 and parts[i+1] else ef.get("default", "").lower()
                    docs_to_insert.append(doc)
                    existing_values.add(main_val)
                if docs_to_insert:
                    try:
                        collection.insert_many(docs_to_insert, ordered=False)
                        st.success(f"Inserted: {len(docs_to_insert)} | Skipped: {skipped}")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                else:
                    st.warning(f"Nothing new to insert. All {skipped} were duplicates or empty.")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2: JOB WORKERS (MARIADB)
# ═══════════════════════════════════════════════════════════════════════════════

with main_tab2:
    st.subheader(f"Job Workers for {org_name}")
    jw_tab1, jw_tab2 = st.tabs(["➕ Add Job Worker", "📋 View Job Workers"])

    with jw_tab1:
        st.markdown("#### Basic Details")
        col1, col2 = st.columns(2)
        with col1:
            jw_name = st.text_input("Job Worker Name *", key="jw_name")
            jw_type = st.selectbox("Type *", list(JOB_WORKER_TYPES.keys()), key="jw_type")
            jw_role = st.selectbox("Role *", list(JOB_WORKER_ROLES.keys()), key="jw_role")
        with col2:
            jw_custom_id = st.text_input("Custom ID * (short unique code, no spaces e.g. MPNA)", key="jw_custom_id")
            jw_capacity = st.number_input("Capacity", min_value=0, value=0, key="jw_capacity")
            jw_gst = st.text_input("GST Number (optional)", key="jw_gst")
            jw_description = st.text_input("Description (optional)", key="jw_description")

        st.markdown("#### Contact Details (optional but appears on challan)")
        col3, col4 = st.columns(2)
        with col3:
            jw_phone = st.text_input("Phone Number", key="jw_phone")
        with col4:
            jw_email = st.text_input("Email", key="jw_email")

        st.markdown("#### Address (optional but appears on challan)")
        col5, col6 = st.columns(2)
        with col5:
            jw_addr_line1 = st.text_input("Address Line 1", key="jw_addr1")
            jw_addr_city = st.text_input("City", key="jw_city")
            jw_addr_country = st.text_input("Country", key="jw_country", value="India")
        with col6:
            jw_addr_line2 = st.text_input("Address Line 2 (optional)", key="jw_addr2")
            jw_addr_state = st.text_input("State", key="jw_state")
            jw_addr_pin = st.text_input("PIN Code", key="jw_pin")
            jw_addr_type = st.selectbox("Address Type", list(ADDRESS_TYPES.keys()), key="jw_addr_type")

        st.markdown("---")
        if st.button("Add Job Worker", type="primary", key="jw_add_btn"):
            if not jw_name.strip():
                st.error("Name is required.")
            elif not jw_custom_id.strip():
                st.error("Custom ID is required.")
            elif " " in jw_custom_id.strip():
                st.error("Custom ID cannot contain spaces. Use something like MPNA or MAPRNT.")
            else:
                try:
                    conn = get_mariadb_conn()
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT job_worker_id FROM job_workers WHERE custom_id = %s", (jw_custom_id.strip(),))
                        if cursor.fetchone():
                            st.error(f"Custom ID '{jw_custom_id}' already exists. Choose a different one.")
                        else:
                            now = datetime.utcnow()
                            cursor.execute("""
                                INSERT INTO job_workers (name, custom_id, type_id, role_id, capacity, gst, pin_code, description, created_at, updated_at)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                jw_name.strip(), jw_custom_id.strip(),
                                JOB_WORKER_TYPES[jw_type], JOB_WORKER_ROLES[jw_role],
                                jw_capacity,
                                jw_gst.strip() if jw_gst.strip() else None,
                                jw_addr_pin.strip() if jw_addr_pin.strip() else None,
                                jw_description.strip() if jw_description.strip() else None,
                                now, now
                            ))
                            new_id = cursor.lastrowid
                            cursor.execute("""
                                INSERT INTO job_worker_organization (job_worker_id, organization_id, assigned_at, is_active)
                                VALUES (%s, %s, %s, %s)
                            """, (new_id, org_id, now, 1))
                            if jw_phone.strip():
                                cursor.execute("""
                                    INSERT INTO job_worker_phones (job_worker_id, phone, type_id, created_at)
                                    VALUES (%s, %s, %s, %s)
                                """, (new_id, jw_phone.strip(), PHONE_TYPE_ID, now))
                            if jw_email.strip():
                                cursor.execute("""
                                    INSERT INTO job_worker_emails (job_worker_id, email, type_id, created_at)
                                    VALUES (%s, %s, %s, %s)
                                """, (new_id, jw_email.strip(), EMAIL_TYPE_ID, now))
                            if jw_addr_line1.strip():
                                cursor.execute("""
                                    INSERT INTO job_worker_addresses (job_worker_id, address_line1, address_line2, city, state, country, pin_code, type_id, created_at, updated_at)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """, (
                                    new_id, jw_addr_line1.strip(),
                                    jw_addr_line2.strip() if jw_addr_line2.strip() else None,
                                    jw_addr_city.strip() if jw_addr_city.strip() else None,
                                    jw_addr_state.strip() if jw_addr_state.strip() else None,
                                    jw_addr_country.strip() if jw_addr_country.strip() else None,
                                    jw_addr_pin.strip() if jw_addr_pin.strip() else None,
                                    ADDRESS_TYPES[jw_addr_type], now, now
                                ))
                            conn.commit()
                            st.success(f"✅ Job worker added successfully (ID: {new_id}) and linked to {org_name}.")
                            st.rerun()
                except Exception as e:
                    st.error(f"Database error: {str(e)}")
                finally:
                    conn.close()

    with jw_tab2:
        try:
            conn = get_mariadb_conn()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT jw.job_worker_id, jw.name, jw.custom_id,
                           jwt.type_name, jwr.role_name, jw.capacity,
                           jw.gst, jwo.is_active
                    FROM job_workers jw
                    JOIN job_worker_organization jwo ON jw.job_worker_id = jwo.job_worker_id
                    JOIN job_worker_type jwt ON jw.type_id = jwt.id
                    JOIN job_worker_role jwr ON jw.role_id = jwr.id
                    WHERE jwo.organization_id = %s
                    ORDER BY jw.job_worker_id DESC
                """, (org_id,))
                rows = cursor.fetchall()
            if not rows:
                st.info(f"No job workers found for {org_name}.")
            else:
                st.caption(f"{len(rows)} job workers found")
                df = pd.DataFrame(rows)
                df.columns = ["ID", "Name", "Custom ID", "Type", "Role", "Capacity", "GST", "Active"]
                df["Active"] = df["Active"].map({1: "✅ Yes", 0: "❌ No"})
                st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Database error: {str(e)}")
        finally:
            conn.close()

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3: SUPPORT TOOLS
# ═══════════════════════════════════════════════════════════════════════════════

with main_tab3:
    st.subheader(f"Support Tools — {org_name}")
    st.caption("Use these tools to resolve customer issues. All destructive actions require confirmation.")

    st_tab1, st_tab2, st_tab3 = st.tabs([
        "🗑️ Delete Design",
        "🔍 Design Journey & Fix",
        "📦 Create Dispatch"
    ])

    # ── TOOL 1: DELETE DESIGN ─────────────────────────────────────────────────
    with st_tab1:
        st.markdown("#### Delete a Design")
        st.caption("Safely deletes a design and all linked records in the correct order.")

        design_search = st.text_input("Design Name or SO Number", placeholder="e.g. DP-124", key="del_search")

        if st.button("Search", key="del_search_btn") and design_search:
            results = run_query(
                "SELECT design_id, design_name, so_number, status, organization_id, design_create_date "
                "FROM designs WHERE (design_name LIKE %s OR so_number LIKE %s) AND organization_id = %s",
                (f"%{design_search}%", f"%{design_search}%", org_id)
            )
            if results:
                st.session_state["del_results"] = results
            else:
                st.error("No design found.")
                st.session_state.pop("del_results", None)

        if "del_results" in st.session_state:
            df = pd.DataFrame(st.session_state["del_results"])
            st.dataframe(df[["design_id", "design_name", "so_number", "status", "design_create_date"]], use_container_width=True)

            selected_id = st.selectbox(
                "Select Design to Delete",
                [r["design_id"] for r in st.session_state["del_results"]],
                format_func=lambda x: next(
                    f"{r['design_name']} | {r['status']} | {r['design_create_date']}"
                    for r in st.session_state["del_results"] if r["design_id"] == x
                ),
                key="del_select"
            )

            if selected_id:
                variants = run_query("SELECT COUNT(*) as cnt FROM product_variants WHERE design_id = %s", (selected_id,))
                fabric = run_query("SELECT COUNT(*) as cnt FROM fabric_transactions WHERE design_id = %s", (selected_id,))
                dept = run_query("SELECT COUNT(*) as cnt FROM design_department_tracking WHERE design_id = %s", (selected_id,))
                items = run_query(
                    "SELECT COUNT(*) as cnt FROM design_item_tracking WHERE tracking_id IN "
                    "(SELECT tracking_id FROM design_department_tracking WHERE design_id = %s)", (selected_id,)
                )

                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Variants", variants[0]["cnt"])
                col2.metric("Fabric Transactions", fabric[0]["cnt"])
                col3.metric("Dept Tracking", dept[0]["cnt"])
                col4.metric("Item Tracking", items[0]["cnt"])

                if fabric[0]["cnt"] > 0:
                    st.error("⛔ Cannot delete — fabric has been issued against this design. Deleting would corrupt inventory records.")
                else:
                    st.warning("⚠️ This action is permanent and cannot be undone.")
                    confirm_del = st.checkbox("I confirm I want to permanently delete this design", key="del_confirm")
                    if confirm_del and st.button("🗑️ Delete Design", type="primary", key="del_btn"):
                        try:
                            run_query(
                                "DELETE FROM design_item_tracking WHERE tracking_id IN "
                                "(SELECT tracking_id FROM design_department_tracking WHERE design_id = %s)",
                                (selected_id,), fetch=False
                            )
                            run_query("DELETE FROM design_department_tracking WHERE design_id = %s", (selected_id,), fetch=False)
                            run_query("DELETE FROM product_variants WHERE design_id = %s", (selected_id,), fetch=False)
                            run_query("DELETE FROM design_ratios WHERE design_id = %s", (selected_id,), fetch=False)
                            run_query("DELETE FROM designs WHERE design_id = %s", (selected_id,), fetch=False)
                            st.success("✅ Design deleted successfully.")
                            st.session_state.pop("del_results", None)
                        except Exception as e:
                            st.error(f"Error: {e}")

    # ── TOOL 2: DESIGN JOURNEY & FIX ──────────────────────────────────────────
    with st_tab2:
        st.markdown("#### Design Journey & Fix")
        st.caption("Search one or multiple designs to see their full department journey, spot where qty is stuck, and fix all in one click.")

        journey_input = st.text_area(
            "Enter Design Name(s) — one per line",
            placeholder="1129\n1122\n4444BOTFSLKBD",
            key="journey_input",
            height=100
        )

        if st.button("🔍 Get Journey Report", key="journey_btn") and journey_input:
            names = [n.strip() for n in journey_input.strip().splitlines() if n.strip()]
            placeholders = ", ".join(["%s"] * len(names))
            # Build LIKE conditions for partial matching
            like_conditions = " OR ".join(["design_name LIKE %s"] * len(names))
            like_params = tuple(f"%{n}%" for n in names) + (org_id,)
            designs = run_query(
                f"SELECT design_id, design_name, status, quantity FROM designs "
                f"WHERE ({like_conditions}) AND organization_id = %s",
                like_params
            )
            if not designs:
                st.error("No designs found for this organisation.")
                st.session_state.pop("journey_data", None)
            else:
                # Build full journey for each design
                all_journey = []
                all_stuck_ids = []
                for design in designs:
                    journey = run_query("""
                        SELECT
                            ddt.tracking_id,
                            dp.name as prev_dept,
                            dc.name as current_dept,
                            ddt.status_id,
                            ddt.processed_date,
                            SUM(dit.quantity) as total_qty
                        FROM design_department_tracking ddt
                        LEFT JOIN departments dp ON dp.department_id = ddt.prev_department_id
                        LEFT JOIN departments dc ON dc.department_id = ddt.current_department_id
                        LEFT JOIN design_item_tracking dit ON dit.tracking_id = ddt.tracking_id
                        WHERE ddt.design_id = %s
                        GROUP BY ddt.tracking_id, dp.name, dc.name, ddt.status_id, ddt.processed_date
                        ORDER BY ddt.processed_date ASC
                    """, (design["design_id"],))
                    all_journey.append({
                        "design": design,
                        "journey": journey
                    })
                    all_stuck_ids.extend([s["tracking_id"] for s in journey if str(s["status_id"]) == '1'])
                st.session_state["journey_data"] = all_journey
                st.session_state["journey_stuck_ids"] = all_stuck_ids
                st.session_state["journey_design_ids"] = [d["design_id"] for d in designs]

        if "journey_data" in st.session_state:
            total_stuck = len(st.session_state["journey_stuck_ids"])

            for item in st.session_state["journey_data"]:
                design = item["design"]
                journey = item["journey"]
                stuck = [s for s in journey if str(s["status_id"]) == '1']

                with st.expander(
                    f"{'🔴' if stuck else '✅'} {design['design_name']} | Status: {design['status']} | Order Qty: {design['quantity']} | {'⚠️ ' + str(len(stuck)) + ' stuck dept(s)' if stuck else 'All clear'}",
                    expanded=bool(stuck)
                ):
                    if not journey:
                        st.info("No tracking records found.")
                        continue

                    # Journey table
                    rows = []
                    prev_qty = None
                    for step in journey:
                        qty_diff = ""
                        if prev_qty is not None and step["total_qty"] is not None and prev_qty is not None:
                            diff = int(step["total_qty"]) - int(prev_qty)
                            if diff < 0:
                                qty_diff = f"⚠️ -{abs(diff)} lost"
                        rows.append({
                            "From": step["prev_dept"] or "START",
                            "To": step["current_dept"],
                            "Qty Transferred": step["total_qty"],
                            "Note": qty_diff,
                            "Status": "✅ COMPLETED" if str(step["status_id"]) == '2' else "🔴 IN PROGRESS",
                            "Date": str(step["processed_date"])
                        })
                        prev_qty = step["total_qty"]

                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

                    # Job worker report
                    st.markdown("##### 👷 Job Worker Report")
                    jw_data = run_query("""
                        SELECT 
                            dc.name as department,
                            jw.name as job_worker,
                            jwr.role_name as role,
                            SUM(vad.issued_quantity) as total_issued,
                            SUM(COALESCE(vad.received_quantity, 0)) as total_received,
                            SUM(vad.issued_quantity - COALESCE(vad.received_quantity, 0)) as qty_not_returned
                        FROM vendor_assignment va
                        JOIN vendor_assignment_details vad ON vad.assignment_id = va.assignment_id
                        JOIN job_workers jw ON jw.job_worker_id = vad.job_worker_id
                        JOIN job_worker_role jwr ON jwr.id = jw.role_id
                        JOIN design_department_tracking ddt ON ddt.tracking_id = va.tracking_id
                        JOIN departments dc ON dc.department_id = ddt.current_department_id
                        WHERE ddt.design_id = %s
                        GROUP BY dc.name, jw.name, jwr.role_name
                        ORDER BY dc.name, qty_not_returned DESC
                    """, (design["design_id"],))

                    if jw_data:
                        jw_rows = []
                        for row in jw_data:
                            not_returned = int(row["qty_not_returned"] or 0)
                            jw_rows.append({
                                "Department": row["department"],
                                "Job Worker": row["job_worker"],
                                "Role": row["role"],
                                "Issued": row["total_issued"],
                                "Received Back": row["total_received"],
                                "Not Returned": f"⚠️ {not_returned}" if not_returned > 0 else "✅ 0"
                            })
                        st.dataframe(pd.DataFrame(jw_rows), use_container_width=True, hide_index=True)
                    else:
                        st.info("No job worker assignment data found for this design.")

                    if stuck:
                        st.error(f"🔴 Stuck at: **{', '.join(s['current_dept'] for s in stuck)}**")
                    else:
                        st.success("✅ All departments completed for this design.")

            # Fix section
            if total_stuck > 0:
                st.markdown("---")
                st.markdown(f"### Fix — {total_stuck} stuck tracking record(s) found")
                st.info("Choose how to fix based on what the customer tells you.")

                fix_col1, fix_col2 = st.columns(2)

                with fix_col1:
                    st.markdown("#### ✅ Option A — Mark All Complete")
                    st.caption("Use when: pieces were actually processed but qty was entered wrong. All pieces exist physically.")
                    confirm_a = st.checkbox("I confirm all departments have physically completed their work", key="journey_confirm_a")
                    if confirm_a and st.button("✅ Mark All as Completed", type="primary", key="journey_fix_btn_a"):
                        try:
                            ids = st.session_state["journey_stuck_ids"]
                            id_ph = ", ".join(["%s"] * len(ids))
                            affected = run_query(
                                f"UPDATE design_department_tracking SET status_id = 2 WHERE tracking_id IN ({id_ph})",
                                tuple(ids), fetch=False
                            )
                            st.success(f"✅ {affected} records marked as COMPLETED. Designs will move out of Tasks in all departments.")
                            st.session_state.pop("journey_data", None)
                            st.session_state.pop("journey_stuck_ids", None)
                            st.session_state.pop("journey_design_ids", None)
                        except Exception as e:
                            st.error(f"Error: {e}")

                with fix_col2:
                    st.markdown("#### ✍️ Option B — Write Off & Close")
                    st.caption("Use when: some pieces are genuinely lost or rejected and will never be recovered. Updates order qty to match reality.")
                    confirm_b = st.checkbox("I confirm the missing pieces are permanently lost and should be written off", key="journey_confirm_b")
                    if confirm_b and st.button("✍️ Write Off & Close", type="primary", key="journey_fix_btn_b"):
                        try:
                            ids = st.session_state["journey_stuck_ids"]
                            design_ids = st.session_state.get("journey_design_ids", [])

                            # For each design, find the minimum qty that actually flowed through
                            # (the last tracking record's qty = what actually made it through)
                            for did in design_ids:
                                last_qty = run_query("""
                                    SELECT SUM(dit.quantity) as total_qty
                                    FROM design_department_tracking ddt
                                    JOIN design_item_tracking dit ON dit.tracking_id = ddt.tracking_id
                                    WHERE ddt.design_id = %s
                                    ORDER BY ddt.processed_date DESC
                                    LIMIT 1
                                """, (did,))
                                # Get the minimum qty seen across all tracking steps (what actually completed)
                                min_qty = run_query("""
                                    SELECT MIN(step_qty) as min_qty FROM (
                                        SELECT SUM(dit.quantity) as step_qty
                                        FROM design_department_tracking ddt
                                        JOIN design_item_tracking dit ON dit.tracking_id = ddt.tracking_id
                                        WHERE ddt.design_id = %s
                                        GROUP BY ddt.tracking_id
                                    ) as qtys
                                """, (did,))
                                if min_qty and min_qty[0]["min_qty"]:
                                    actual_qty = min_qty[0]["min_qty"]
                                    run_query(
                                        "UPDATE designs SET quantity = %s WHERE design_id = %s",
                                        (actual_qty, did), fetch=False
                                    )

                            # Mark all stuck as completed
                            id_ph = ", ".join(["%s"] * len(ids))
                            affected = run_query(
                                f"UPDATE design_department_tracking SET status_id = 2 WHERE tracking_id IN ({id_ph})",
                                tuple(ids), fetch=False
                            )
                            st.success(f"✅ {affected} records marked as COMPLETED and order quantities updated to reflect actual processed pieces. Designs will move out of Tasks.")
                            st.session_state.pop("journey_data", None)
                            st.session_state.pop("journey_stuck_ids", None)
                            st.session_state.pop("journey_design_ids", None)
                        except Exception as e:
                            st.error(f"Error: {e}")
            else:
                st.success("✅ No stuck records found across all designs.")

    # ── TOOL 3: CREATE DISPATCH ────────────────────────────────────────────────
    with st_tab3:
        st.markdown("#### Create Dispatch Records")
        st.caption("Use when designs have been physically dispatched but the app was never updated.")

        dispatch_names = st.text_area("Enter Design Names (one per line)", placeholder="1129\n1122\n1133", key="dispatch_names")

        col1, col2 = st.columns(2)
        with col1:
            dispatch_date = st.date_input("Dispatch Date", value=datetime.today(), key="dispatch_date")
        with col2:
            dispatch_time = st.time_input("Dispatch Time", value=datetime.now().time(), key="dispatch_time")

        if st.button("Load Designs & Employees", key="dispatch_load_btn") and dispatch_names:
            names = [n.strip() for n in dispatch_names.strip().splitlines() if n.strip()]
            placeholders = ", ".join(["%s"] * len(names))
            designs = run_query(
                f"SELECT design_id, design_name, quantity FROM designs "
                f"WHERE design_name IN ({placeholders}) AND organization_id = %s",
                tuple(names) + (org_id,)
            )
            employees = run_query(
                "SELECT e.employee_id, u.full_name, e.role FROM employees e "
                "JOIN users u ON u.user_id = e.user_id "
                "WHERE e.organization_id = %s AND e.status = 'ONBOARDED' ORDER BY e.role",
                (org_id,)
            )
            if not designs:
                st.error("No designs found.")
            else:
                st.session_state["dispatch_designs"] = designs
                st.session_state["dispatch_employees"] = employees

        if "dispatch_designs" in st.session_state and "dispatch_employees" in st.session_state:
            emp_options = {
                e["employee_id"]: f"{e['full_name']} ({e['role']})"
                for e in st.session_state["dispatch_employees"]
            }

            col1, col2 = st.columns(2)
            with col1:
                dispatched_by = st.selectbox("Dispatched By", list(emp_options.keys()), format_func=lambda x: emp_options[x], key="disp_by")
            with col2:
                approved_by = st.selectbox("Approved By", list(emp_options.keys()), format_func=lambda x: emp_options[x], key="disp_approved")

            df = pd.DataFrame(st.session_state["dispatch_designs"])
            st.dataframe(df, use_container_width=True)

            # Check for existing dispatch records
            design_ids = [d["design_id"] for d in st.session_state["dispatch_designs"]]
            id_ph = ", ".join(["%s"] * len(design_ids))
            existing = run_query(f"SELECT design_id FROM dispatch_records WHERE design_id IN ({id_ph})", tuple(design_ids))
            existing_ids = {e["design_id"] for e in existing}
            new_designs = [d for d in st.session_state["dispatch_designs"] if d["design_id"] not in existing_ids]

            if existing_ids:
                st.warning(f"⚠️ {len(existing_ids)} design(s) already have dispatch records and will be skipped.")
            if not new_designs:
                st.error("All selected designs already have dispatch records.")
            else:
                st.info(f"ℹ️ Will create dispatch records for {len(new_designs)} design(s). Quantities pulled from Cutting department.")
                confirm_dispatch = st.checkbox("I confirm these designs have been physically dispatched", key="dispatch_confirm")

                if confirm_dispatch and st.button("📦 Create Dispatch Records", type="primary", key="dispatch_create_btn"):
                    try:
                        dispatch_dt = datetime.combine(dispatch_date, dispatch_time).strftime("%Y-%m-%d %H:%M:%S")
                        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        cutting_dept = run_query(
                            "SELECT department_id FROM departments WHERE org_id = %s AND department_type = 'Cutting' AND is_active = 1 LIMIT 1",
                            (org_id,)
                        )
                        if not cutting_dept:
                            st.error("No active Cutting department found for this organisation.")
                            st.stop()
                        cutting_dept_id = cutting_dept[0]["department_id"]

                        success_count = 0
                        for design in new_designs:
                            did = design["design_id"]

                            last_tracking = run_query(
                                "SELECT tracking_id FROM design_department_tracking WHERE design_id = %s ORDER BY processed_date DESC LIMIT 1",
                                (did,)
                            )
                            if not last_tracking:
                                continue
                            tracking_id = last_tracking[0]["tracking_id"]

                            run_query(
                                "INSERT INTO dispatch_records (design_id, tracking_id, dispatched_by, dispatch_date, "
                                "approval_status_id, approved_by, approval_date, created_at, party_id) "
                                "VALUES (%s, %s, %s, %s, 1, %s, %s, %s, NULL)",
                                (did, tracking_id, dispatched_by, dispatch_dt, approved_by, dispatch_dt, now),
                                fetch=False
                            )

                            dispatch_rec = run_query(
                                "SELECT dispatch_id FROM dispatch_records WHERE design_id = %s AND dispatched_by = %s ORDER BY dispatch_id DESC LIMIT 1",
                                (did, dispatched_by)
                            )
                            dispatch_id = dispatch_rec[0]["dispatch_id"]

                            cutting_variants = run_query("""
                                SELECT pv.variant_id, dit.quantity
                                FROM design_department_tracking ddt
                                JOIN design_item_tracking dit ON dit.tracking_id = ddt.tracking_id
                                JOIN product_variants pv ON pv.variant_id = dit.variant_id
                                WHERE ddt.design_id = %s AND ddt.current_department_id = %s
                            """, (did, cutting_dept_id))

                            if cutting_variants:
                                item_data = [(dispatch_id, v["variant_id"], v["quantity"], now) for v in cutting_variants]
                                run_many(
                                    "INSERT INTO dispatch_items (dispatch_id, variant_id, quantity, created_at) VALUES (%s, %s, %s, %s)",
                                    item_data
                                )
                            success_count += 1

                        st.success(f"✅ Dispatch records created for {success_count} design(s).")
                        st.session_state.pop("dispatch_designs", None)
                        st.session_state.pop("dispatch_employees", None)

                    except Exception as e:
                        st.error(f"Error: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4: PURCHASE MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

with main_tab4:
    st.subheader(f"Purchase Management — {org_name}")
    st.caption("Manage purchase invoices. All destructive actions require confirmation.")

    pm_tab1, pm_tab2, pm_tab3, pm_tab4, pm_tab5 = st.tabs([
        "🗑️ Delete Invoice",
        "🔍 View Invoice",
        "🎨 Change Fabric Type",
        "🎨 Change Colour",
        "💣 Force Delete Lot/Invoice"
    ])

    # ── TOOL 1: DELETE INVOICE ────────────────────────────────────────────────
    with pm_tab1:
        st.markdown("#### Delete Invoice")
        st.caption("Search by invoice number, select all or specific POs, then delete safely.")

        invoice_search = st.text_input("Invoice Number", placeholder="e.g. DE/0442/25-26", key="inv_search")

        if st.button("🔍 Search Invoice", key="inv_search_btn") and invoice_search:
            purchases = run_query(
                "SELECT purchase_id, invoice_no, po, vendor_name, purchase_date, total_qty "
                "FROM purchases WHERE invoice_no LIKE %s AND org_id = %s "
                "ORDER BY po, purchase_date",
                (f"%{invoice_search}%", org_id)
            )
            if not purchases:
                st.error("No invoice found.")
                st.session_state.pop("inv_purchases", None)
            else:
                st.session_state["inv_purchases"] = purchases
                st.session_state.pop("inv_selected_ids", None)

        if "inv_purchases" in st.session_state:
            purchases = st.session_state["inv_purchases"]

            # Group by PO for display
            po_list = sorted(list(set(p["po"] for p in purchases)))
            st.markdown(f"**Found {len(purchases)} record(s) under {len(po_list)} PO(s):**")

            # PO selector
            po_options = ["All POs"] + po_list
            selected_po = st.selectbox("Select PO to delete (or All POs)", po_options, key="inv_po_select")

            # Filter based on selection
            if selected_po == "All POs":
                filtered_purchases = purchases
            else:
                filtered_purchases = [p for p in purchases if p["po"] == selected_po]

            # Show filtered records
            df = pd.DataFrame(filtered_purchases)
            st.dataframe(df[["invoice_no", "po", "vendor_name", "purchase_date", "total_qty"]], use_container_width=True, hide_index=True)

            # Check fabric transactions
            filtered_ids = [p["purchase_id"] for p in filtered_purchases]
            id_ph = ", ".join(["%s"] * len(filtered_ids))

            fabric_count = run_query(
                f"SELECT COUNT(*) as cnt FROM fabric_transactions ft "
                f"JOIN purchase_entries pe ON pe.sl_no = ft.sl_no "
                f"WHERE pe.purchase_id IN ({id_ph})",
                tuple(filtered_ids)
            )
            entries_count = run_query(
                f"SELECT COUNT(*) as cnt FROM purchase_entries WHERE purchase_id IN ({id_ph})",
                tuple(filtered_ids)
            )

            col1, col2, col3 = st.columns(3)
            col1.metric("Purchase Records", len(filtered_purchases))
            col2.metric("Purchase Entries", entries_count[0]["cnt"])
            col3.metric("Fabric Issued", fabric_count[0]["cnt"])

            if int(fabric_count[0]["cnt"]) > 0:
                st.error(f"⛔ Cannot delete — fabric from this selection has already been issued to production ({fabric_count[0]['cnt']} transactions). Deleting would corrupt inventory records.")
            else:
                st.warning(f"⚠️ This will permanently delete {len(filtered_purchases)} purchase record(s) and {entries_count[0]['cnt']} entries. This cannot be undone.")
                confirm_inv = st.checkbox(
                    f"I confirm I want to permanently delete {'all POs' if selected_po == 'All POs' else 'PO ' + selected_po} under this invoice",
                    key="inv_confirm"
                )

                if confirm_inv and st.button("🗑️ Delete", type="primary", key="inv_delete_btn"):
                    try:
                        id_ph = ", ".join(["%s"] * len(filtered_ids))

                        entries_deleted = run_query(
                            f"DELETE FROM purchase_entries WHERE purchase_id IN ({id_ph})",
                            tuple(filtered_ids), fetch=False
                        )
                        purchases_deleted = run_query(
                            f"DELETE FROM purchases WHERE purchase_id IN ({id_ph})",
                            tuple(filtered_ids), fetch=False
                        )
                        st.success(f"✅ Deleted — {purchases_deleted} purchase record(s) and {entries_deleted} entries removed.")
                        st.session_state.pop("inv_purchases", None)
                    except Exception as e:
                        st.error(f"Error: {e}")

    # ── TOOL 2: VIEW INVOICE ──────────────────────────────────────────────────
    with pm_tab2:
        st.markdown("#### View Invoice Details")
        st.caption("Search and view all entries under an invoice.")

        view_invoice = st.text_input("Invoice Number", placeholder="e.g. DE/0466/25-26", key="inv_view_search")

        if st.button("🔍 View", key="inv_view_btn") and view_invoice:
            results = run_query("""
                SELECT 
                    p.invoice_no,
                    p.po,
                    p.vendor_name,
                    p.purchase_date,
                    p.total_qty,
                    pe.transaction_id,
                    pe.sl_no,
                    pe.weight,
                    pe.created_at
                FROM purchases p
                JOIN purchase_entries pe ON pe.purchase_id = p.purchase_id
                WHERE p.invoice_no LIKE %s AND p.org_id = %s
                ORDER BY p.po, pe.sl_no
            """, (f"%{view_invoice}%", org_id))

            if not results:
                st.error("No invoice found.")
            else:
                df = pd.DataFrame(results)
                st.caption(f"Found {len(results)} entries")
                st.dataframe(df, use_container_width=True, hide_index=True)

    # ── TOOL 3: CHANGE FABRIC TYPE ────────────────────────────────────────────
    with pm_tab3:
        st.markdown("#### Change Fabric Type")
        st.caption("Change fabric type for one or more lot numbers. Updates both MongoDB (inventory) and MySQL (purchase records).")

        lot_input = st.text_area(
            "Enter Lot Number(s) — one per line",
            placeholder="FFA0834\nFFA0835\nFFA0872",
            key="fabric_lot_input",
            height=100
        )

        if st.button("🔍 Find Lots", key="fabric_find_btn") and lot_input:
            lots = [l.strip() for l in lot_input.strip().splitlines() if l.strip()]

            # Search MongoDB
            audit_col = get_audit_db()
            mongo_results = list(audit_col.aggregate([
                {"$match": {
                    "lot_no": {"$in": lots},
                    "organization_id": org_id
                }},
                {"$group": {
                    "_id": "$lot_no",
                    "fabric_type": {"$first": "$fabric_type"},
                    "count": {"$sum": 1},
                    "total_weight": {"$sum": "$weight"}
                }},
                {"$sort": {"_id": 1}}
            ]))

            if not mongo_results:
                st.error("No lots found in inventory for this organisation.")
                st.session_state.pop("fabric_lots", None)
            else:
                st.session_state["fabric_lots"] = lots
                st.session_state["fabric_mongo_results"] = mongo_results

        if "fabric_mongo_results" in st.session_state:
            st.markdown("**Lots found:**")
            rows = []
            for lot in st.session_state["fabric_mongo_results"]:
                rows.append({
                    "Lot No": lot["_id"],
                    "Current Fabric Type": lot["fabric_type"],
                    "Roll Count": lot["count"],
                    "Total Weight (kg)": round(lot["total_weight"], 2)
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

            # Get available fabric types from MongoDB
            fabric_types_col = get_mongo_db()["fabric_types"]
            fabric_type_docs = list(fabric_types_col.find(
                {"organization_id": org_id},
                {"fabric_type": 1, "_id": 0}
            ))
            fabric_type_options = [f["fabric_type"].upper() for f in fabric_type_docs]

            col1, col2 = st.columns(2)
            with col1:
                new_fabric_type = st.selectbox(
                    "Select New Fabric Type",
                    fabric_type_options,
                    key="fabric_new_type"
                )
            with col2:
                custom_type = st.text_input(
                    "Or type custom fabric type",
                    placeholder="e.g. LOOPKNIT",
                    key="fabric_custom_type"
                )

            # Use custom if provided, otherwise use dropdown
            final_fabric_type = custom_type.strip().upper() if custom_type.strip() else new_fabric_type

            if final_fabric_type:
                st.info(f"Will change fabric type to: **{final_fabric_type}**")

            confirm_fabric = st.checkbox(
                "I confirm I want to change the fabric type for all the above lots",
                key="fabric_confirm"
            )

            if confirm_fabric and st.button("🎨 Update Fabric Type", type="primary", key="fabric_update_btn"):
                try:
                    lots = st.session_state["fabric_lots"]
                    audit_col = get_audit_db()

                    # Update MongoDB
                    mongo_result = audit_col.update_many(
                        {"lot_no": {"$in": lots}, "organization_id": org_id},
                        {"$set": {"fabric_type": final_fabric_type}}
                    )

                    # Update MySQL purchases.details for matching purchase_ids
                    # Find purchase_ids from MongoDB results
                    purchase_ids = list(audit_col.distinct(
                        "purchase_id",
                        {"lot_no": {"$in": lots}, "organization_id": org_id}
                    ))

                    mysql_affected = 0
                    if purchase_ids:
                        id_ph = ", ".join(["%s"] * len(purchase_ids))
                        mysql_affected = run_query(
                            f"UPDATE purchases SET details = %s WHERE purchase_id IN ({id_ph}) AND org_id = %s",
                            tuple([final_fabric_type] + purchase_ids + [org_id]),
                            fetch=False
                        )

                    st.success(
                        f"✅ Updated successfully!\n\n"
                        f"- MongoDB: **{mongo_result.modified_count} documents** updated\n"
                        f"- MySQL: **{mysql_affected} purchase records** updated\n\n"
                        f"Tell the customer to force close and reopen the app."
                    )
                    st.session_state.pop("fabric_lots", None)
                    st.session_state.pop("fabric_mongo_results", None)

                except Exception as e:
                    st.error(f"Error: {e}")

    # ── TOOL 4: CHANGE COLOUR ─────────────────────────────────────────────────
    with pm_tab4:
        st.markdown("#### Change Colour of Lot")
        st.caption("Change the colour for one or more lot numbers in inventory.")

        clr_lot_input = st.text_area(
            "Enter Lot Number(s) — one per line",
            placeholder="FFA0834\nFFA0835",
            key="clr_lot_input",
            height=100
        )

        if st.button("🔍 Find Lots", key="clr_find_btn") and clr_lot_input:
            lots = [l.strip() for l in clr_lot_input.strip().splitlines() if l.strip()]
            audit_col = get_audit_db()
            clr_results = list(audit_col.aggregate([
                {"$match": {"lot_no": {"$in": lots}, "organization_id": org_id}},
                {"$group": {
                    "_id": "$lot_no",
                    "current_colour": {"$first": "$clr"},
                    "fabric_type": {"$first": "$fabric_type"},
                    "count": {"$sum": 1}
                }},
                {"$sort": {"_id": 1}}
            ]))

            if not clr_results:
                st.error("No lots found in inventory for this organisation.")
                st.session_state.pop("clr_lots", None)
            else:
                st.session_state["clr_lots"] = lots
                st.session_state["clr_results"] = clr_results

        if "clr_results" in st.session_state:
            rows = []
            for lot in st.session_state["clr_results"]:
                rows.append({
                    "Lot No": lot["_id"],
                    "Current Colour": lot["current_colour"],
                    "Fabric Type": lot["fabric_type"],
                    "Roll Count": lot["count"]
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

            # Get available colours from MongoDB
            colours_col = get_mongo_db()["colors"]
            colour_docs = list(colours_col.find(
                {"organization_id": org_id},
                {"color": 1, "_id": 0}
            ))
            colour_options = [c["color"].upper() for c in colour_docs]

            col1, col2 = st.columns(2)
            with col1:
                new_colour = st.selectbox("Select New Colour", colour_options, key="clr_new_colour")
            with col2:
                custom_colour = st.text_input("Or type custom colour", placeholder="e.g. NAVY BLUE", key="clr_custom_colour")

            final_colour = custom_colour.strip().upper() if custom_colour.strip() else new_colour

            if final_colour:
                st.info(f"Will change colour to: **{final_colour}**")

            confirm_clr = st.checkbox("I confirm I want to change the colour for all above lots", key="clr_confirm")

            if confirm_clr and st.button("🎨 Update Colour", type="primary", key="clr_update_btn"):
                try:
                    lots = st.session_state["clr_lots"]
                    audit_col = get_audit_db()
                    result = audit_col.update_many(
                        {"lot_no": {"$in": lots}, "organization_id": org_id},
                        {"$set": {"clr": final_colour}}
                    )
                    st.success(f"✅ Colour updated for **{result.modified_count} documents**. Ask customer to reopen the app.")
                    st.session_state.pop("clr_lots", None)
                    st.session_state.pop("clr_results", None)
                except Exception as e:
                    st.error(f"Error: {e}")

    # ── TOOL 5: FORCE DELETE LOT/INVOICE ─────────────────────────────────────
    with pm_tab5:
        st.markdown("#### Force Delete Lot / Invoice")
        st.caption("Completely wipe a lot or invoice from inventory — even if fabric has been issued. Use when fabric needs to be re-weighed and re-entered fresh.")

        fd_tab1, fd_tab2 = st.tabs(["By Lot Number", "By Invoice Number"])

        with fd_tab1:
            fd_lot_input = st.text_area(
                "Enter Lot Number(s) — one per line",
                placeholder="FFA0834\nFFA0835",
                key="fd_lot_input",
                height=100
            )

            if st.button("🔍 Check Lot", key="fd_lot_find_btn") and fd_lot_input:
                lots = [l.strip() for l in fd_lot_input.strip().splitlines() if l.strip()]
                audit_col = get_audit_db()

                # Get all sl_nos for these lots from MongoDB
                mongo_docs = list(audit_col.find(
                    {"lot_no": {"$in": lots}, "organization_id": org_id},
                    {"sl_no": 1, "lot_no": 1, "clr": 1, "fabric_type": 1, "location": 1, "purchase_id": 1}
                ))

                if not mongo_docs:
                    st.error("No lots found in inventory.")
                    st.session_state.pop("fd_lot_data", None)
                else:
                    sl_nos = [d["sl_no"] for d in mongo_docs]
                    purchase_ids = list(set(d["purchase_id"] for d in mongo_docs if d.get("purchase_id")))

                    # Check fabric transactions
                    if sl_nos:
                        sl_ph = ", ".join(["%s"] * len(sl_nos))
                        fabric_txns = run_query(
                            f"SELECT transaction_id, sl_no, status, design_id FROM fabric_transactions WHERE sl_no IN ({sl_ph})",
                            tuple(sl_nos)
                        )
                    else:
                        fabric_txns = []

                    issued = [t for t in fabric_txns if t["status"] == "ISSUED"]
                    returned = [t for t in fabric_txns if t["status"] == "RETURNED"]

                    st.session_state["fd_lot_data"] = {
                        "lots": lots,
                        "sl_nos": sl_nos,
                        "purchase_ids": purchase_ids,
                        "mongo_count": len(mongo_docs),
                        "issued_count": len(issued),
                        "returned_count": len(returned),
                        "fabric_txn_count": len(fabric_txns)
                    }

            if "fd_lot_data" in st.session_state:
                d = st.session_state["fd_lot_data"]
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total Rolls", d["mongo_count"])
                col2.metric("Fabric Transactions", d["fabric_txn_count"])
                col3.metric("Currently Issued", d["issued_count"])
                col4.metric("Returned", d["returned_count"])

                if d["issued_count"] > 0:
                    st.warning(f"⚠️ {d['issued_count']} roll(s) are currently issued to departments. All issuance records will also be deleted.")

                st.error("🚨 This will permanently delete ALL records for these lots from inventory, purchase entries, and fabric transactions. This cannot be undone.")

                confirm_fd1 = st.checkbox("I understand this is permanent and the fabric will need to be re-entered fresh", key="fd_lot_confirm1")
                confirm_fd2 = st.checkbox("I confirm the customer has approved this deletion", key="fd_lot_confirm2")

                if confirm_fd1 and confirm_fd2 and st.button("💣 Force Delete Lots", type="primary", key="fd_lot_delete_btn"):
                    try:
                        sl_nos = d["sl_nos"]
                        purchase_ids = d["purchase_ids"]
                        audit_col = get_audit_db()

                        # Step 1 — Delete fabric transactions from MySQL
                        ft_deleted = 0
                        if sl_nos:
                            sl_ph = ", ".join(["%s"] * len(sl_nos))
                            ft_deleted = run_query(
                                f"DELETE FROM fabric_transactions WHERE sl_no IN ({sl_ph})",
                                tuple(sl_nos), fetch=False
                            )

                        # Step 2 — Delete purchase entries from MySQL
                        pe_deleted = 0
                        if sl_nos:
                            pe_deleted = run_query(
                                f"DELETE FROM purchase_entries WHERE sl_no IN ({sl_ph})",
                                tuple(sl_nos), fetch=False
                            )

                        # Step 3 — Delete purchases from MySQL (only if all entries deleted)
                        p_deleted = 0
                        if purchase_ids:
                            pid_ph = ", ".join(["%s"] * len(purchase_ids))
                            # Only delete purchase if no remaining entries
                            for pid in purchase_ids:
                                remaining = run_query(
                                    "SELECT COUNT(*) as cnt FROM purchase_entries WHERE purchase_id = %s",
                                    (pid,)
                                )
                                if remaining[0]["cnt"] == 0:
                                    run_query("DELETE FROM purchases WHERE purchase_id = %s", (pid,), fetch=False)
                                    p_deleted += 1

                        # Step 4 — Delete from MongoDB
                        mongo_result = audit_col.delete_many(
                            {"lot_no": {"$in": d["lots"]}, "organization_id": org_id}
                        )

                        st.success(
                            f"✅ Force delete complete!\n\n"
                            f"- Fabric transactions deleted: **{ft_deleted}**\n"
                            f"- Purchase entries deleted: **{pe_deleted}**\n"
                            f"- Purchase records deleted: **{p_deleted}**\n"
                            f"- MongoDB inventory records deleted: **{mongo_result.deleted_count}**"
                        )
                        st.session_state.pop("fd_lot_data", None)
                    except Exception as e:
                        st.error(f"Error: {e}")

        with fd_tab2:
            fd_inv_input = st.text_input("Invoice Number", placeholder="e.g. KT/522/25-26", key="fd_inv_input")

            if st.button("🔍 Check Invoice", key="fd_inv_find_btn") and fd_inv_input:
                purchases = run_query(
                    "SELECT purchase_id, invoice_no, po, details, total_qty FROM purchases "
                    "WHERE invoice_no LIKE %s AND org_id = %s",
                    (f"%{fd_inv_input}%", org_id)
                )
                if not purchases:
                    st.error("Invoice not found.")
                    st.session_state.pop("fd_inv_data", None)
                else:
                    purchase_ids = [p["purchase_id"] for p in purchases]
                    id_ph = ", ".join(["%s"] * len(purchase_ids))

                    # Get all sl_nos
                    entries = run_query(
                        f"SELECT sl_no FROM purchase_entries WHERE purchase_id IN ({id_ph})",
                        tuple(purchase_ids)
                    )
                    sl_nos = [e["sl_no"] for e in entries]

                    # Check fabric transactions
                    ft_count = 0
                    issued_count = 0
                    if sl_nos:
                        sl_ph = ", ".join(["%s"] * len(sl_nos))
                        ft_data = run_query(
                            f"SELECT status, COUNT(*) as cnt FROM fabric_transactions WHERE sl_no IN ({sl_ph}) GROUP BY status",
                            tuple(sl_nos)
                        )
                        for row in ft_data:
                            ft_count += row["cnt"]
                            if row["status"] == "ISSUED":
                                issued_count += row["cnt"]

                    st.session_state["fd_inv_data"] = {
                        "purchase_ids": purchase_ids,
                        "sl_nos": sl_nos,
                        "purchases": purchases,
                        "ft_count": ft_count,
                        "issued_count": issued_count
                    }

            if "fd_inv_data" in st.session_state:
                d = st.session_state["fd_inv_data"]
                df = pd.DataFrame(d["purchases"])
                st.dataframe(df[["invoice_no", "po", "details", "total_qty"]], use_container_width=True, hide_index=True)

                col1, col2, col3 = st.columns(3)
                col1.metric("Total Rolls", len(d["sl_nos"]))
                col2.metric("Fabric Transactions", d["ft_count"])
                col3.metric("Currently Issued", d["issued_count"])

                if d["issued_count"] > 0:
                    st.warning(f"⚠️ {d['issued_count']} roll(s) currently issued. All issuance records will be deleted.")

                st.error("🚨 This will permanently delete ALL records for this invoice. Cannot be undone.")

                confirm_fi1 = st.checkbox("I understand this is permanent and fabric will need to be re-entered", key="fd_inv_confirm1")
                confirm_fi2 = st.checkbox("I confirm the customer has approved this deletion", key="fd_inv_confirm2")

                if confirm_fi1 and confirm_fi2 and st.button("💣 Force Delete Invoice", type="primary", key="fd_inv_delete_btn"):
                    try:
                        sl_nos = d["sl_nos"]
                        purchase_ids = d["purchase_ids"]
                        audit_col = get_audit_db()

                        # Step 1 — Delete fabric transactions
                        ft_deleted = 0
                        if sl_nos:
                            sl_ph = ", ".join(["%s"] * len(sl_nos))
                            ft_deleted = run_query(
                                f"DELETE FROM fabric_transactions WHERE sl_no IN ({sl_ph})",
                                tuple(sl_nos), fetch=False
                            )

                        # Step 2 — Delete purchase entries
                        pe_deleted = 0
                        if purchase_ids:
                            id_ph = ", ".join(["%s"] * len(purchase_ids))
                            pe_deleted = run_query(
                                f"DELETE FROM purchase_entries WHERE purchase_id IN ({id_ph})",
                                tuple(purchase_ids), fetch=False
                            )

                        # Step 3 — Delete purchases
                        p_deleted = 0
                        if purchase_ids:
                            p_deleted = run_query(
                                f"DELETE FROM purchases WHERE purchase_id IN ({id_ph})",
                                tuple(purchase_ids), fetch=False
                            )

                        # Step 4 — Delete from MongoDB using purchase_ids
                        mongo_result = audit_col.delete_many(
                            {"purchase_id": {"$in": purchase_ids}, "organization_id": org_id}
                        )

                        st.success(
                            f"✅ Force delete complete!\n\n"
                            f"- Fabric transactions deleted: **{ft_deleted}**\n"
                            f"- Purchase entries deleted: **{pe_deleted}**\n"
                            f"- Purchase records deleted: **{p_deleted}**\n"
                            f"- MongoDB inventory records deleted: **{mongo_result.deleted_count}**"
                        )
                        st.session_state.pop("fd_inv_data", None)
                    except Exception as e:
                        st.error(f"Error: {e}")
