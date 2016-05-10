(in-package :ctcl)

(defun db-do-query (query)
  (psql-do-query(query)))

(defun db-ensure-connection (db)
  (psql-ensure-connection (db)))


(defun db-create-tables ()
  (psql-create-table))


(defun psql-do-query (query)
  ;;(format t "db-hit:~A~%" query)
  (let ((database "metis")
	(user-name "metis")
	(password "metis")
	(host "localhost"))
    (ignore-errors
      (postmodern:with-connection `(,database ,user-name ,password ,host :pooled-p t)
	(postmodern:query query)))))

(defun psql-drop-table (table)
  (ignore-errors
    (psql-do-query (format nil "drop table if exists ~A cascade" table))))

(defun psql-ensure-connection (db)
  (unless postmodern:*database*
    (setf postmodern:*database* (postmodern:connect db "metis" "metis" "localhost" :pooled-p t))))



(defun psql-create-tables ()
  (let ((tables '(:event_names :event_times :files :source_hosts :user_agents :user_names :user_keys )))
    (ignore-errors
      (mapcar #'psql-drop-table tables)
      (psql-do-query "drop table if exists log"))
    (mapcar #'psql-create-table tables))
  (psql-do-query "delete from metrics")
  (psql-do-query "create table if not exists log(id serial, event_time integer, user_name integer, user_key integer, event_name integer, user_agent integer, source_host integer)")
  (psql-do-query "create or replace view ct as select event_names.value as event, event_times.value as etime, source_hosts.value as source, user_agents.value as agent, user_names.value as name, user_keys.value as key from event_names,log,event_times,source_hosts,user_agents,user_names,user_keys where event_names.id = log.event_name and event_times.id = log.event_time and source_hosts.id = log.source_host and user_agents.id = log.user_agent and user_names.id = log.user_name and user_keys.id = log.user_key;"))

(defun psql-create-table (table)
  (psql-do-query (format nil "create table if not exists ~A(id serial, value text)" table))
  (psql-do-query (format nil "create unique index ~A_idx1 on ~A(id)" table table))
  (psql-do-query (format nil "create unique index ~A_idx2 on ~A(value)" table table)))


(fare-memoization:define-memo-function get-id-or-insert-psql (table value)
  (let ((id
	 (flatten
	  (psql-do-query
	   (format nil "select id from ~A where value = '~A'" table value)))))
    (if (not id)
	(progn
	  (psql-do-query (format nil "insert into ~A(value) values('~A')" table value))
	  (setq id (car (car (psql-do-query (format nil "select id from ~A where value = '~A'" table value)))))
	  id)
	id)))

(defun normalize-insert (event-time user-name user-key event-name user-agent source-host)
  (let* ((event-time-id (get-id-or-insert-psql "event_times" event-time))
	 (user-name-id (get-id-or-insert-psql "user_names" user-name))
	 (user-key-id (get-id-or-insert-psql "user_keys" user-key))
	 ;;(user-identity-id (get-id-or-insert-psql "user_identitys " user-identity))
	 (event-name-id (get-id-or-insert-psql "event_names" event-name))
	 (user-agent-id (get-id-or-insert-psql "user_agents" user-agent))
	 (source-host-id (get-id-or-insert-psql "source_hosts" source-host)))
    (psql-do-query
     (format nil "insert into log(event_time,user_name,user_key,event_name,user_agent,source_host) values ('~A','~A','~A','~A','~A','~A')"
	     event-time-id user-name-id user-key-id event-name-id user-agent-id source-host-id))))

(defun load-file-values ()
  (unless *files*
    (progn
      (setf *files* (psql-do-query "select value from files"))
      (mapcar #'(lambda (x)
                  (setf (gethash (car x) *h*) t))
	      *files*)))
  *h*)
