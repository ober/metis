(in-package :metis)

;;(defparameter *q* (make-instance 'queue))
(defvar *h* (make-hash-table :test 'equalp))
(defvar *DB* nil)
(defvar *pcallers* 5)
(defvar dbtype "postgres")
(defvar *files* nil)

(defun db-have-we-seen-this-file (x)
  (format t ".")
  (if (psql-do-query (format nil "select id from files where value = '~A'" (file-namestring x)))
      t
      nil))

(defun db-mark-file-processed (x)
  (psql-do-query
   (format nil "insert into files(value) values ('~A')" (file-namestring x)))
  (setf (gethash (file-namestring x) *h*) t))

(defun db-mark-file-processed-preload (x)
  (psql-do-query
   (format nil "insert into files(value) values ('~A')" (file-namestring x))))

(defun psql-do-query (query &optional db)
  (let ((database (or db "metis"))
	(user-name "metis")
	(password "metis")
	(host "localhost"))
    (postmodern:with-connection
	`(,database ,user-name ,password ,host :pooled-p t)
	(postmodern:query query))))

(defun psql-do-trans (query &optional db)
  (let ((database (or db "metis"))
	(user-name "metis")
	(password "metis")
	(host "localhost"))
    (postmodern:with-connection
	`(,database ,user-name ,password ,host :pooled-p t)
      (postmodern:with-transaction ()
	(postmodern:query query)))))

(defun psql-drop-table (table &optional db)
  (let ((database (or db "metis")))
    (format t "dt: ~A db:~A~%" table db)
    (psql-do-query (format nil "drop table if exists ~A cascade" table) database)))

(defun psql-ensure-connection (&optional db)
  (unless postmodern:*database*
    (setf postmodern:*database*
	  (postmodern:connect
	   (or db "metis")
	   "metis" "metis" "localhost" :pooled-p t))))

(defun psql-recreate-tables (&optional db)
  (let ((database (or db "metis"))
	(tables '(:event_names :event_times :files :source_hosts :user_agents :user_names :user_keys )))
    (mapcar
     #'(lambda (x)
	 (psql-drop-table x database)) tables)
    (psql-do-query "drop table if exists log" database)
    (mapcar
     #'(lambda (x)
	 (psql-create-table x database)) tables)
    (psql-do-query "create table if not exists log(id serial, event_time integer, user_name integer, user_key integer, event_name integer, user_agent integer, source_host integer)" database)
    (psql-do-query "create or replace view ct as select event_names.value as event, event_times.value as etime, source_hosts.value as source, user_agents.value as agent, user_names.value as name, user_keys.value as key from event_names,log,event_times,source_hosts,user_agents,user_names,user_keys where event_names.id = log.event_name and event_times.id = log.event_time and source_hosts.id = log.source_host and user_agents.id = log.user_agent and user_names.id = log.user_name and user_keys.id = log.user_key;" database)))

(defun psql-create-tables (&optional db)
  (let ((tables '(:event_names :event_times :files :source_hosts :user_agents :user_names :user_keys ))
	(database (or db "metis")))
    (mapcar #'(lambda (x)
		(psql-create-table x db)) tables)
    (psql-do-query "create table if not exists log(id serial, event_time integer, user_name integer, user_key integer, event_name integer, user_agent integer, source_host integer)" database)
    (psql-do-query "create or replace view ct as select event_names.value as event, event_times.value as etime, source_hosts.value as source, user_agents.value as agent, user_names.value as name, user_keys.value as key from event_names,log,event_times,source_hosts,user_agents,user_names,user_keys where event_names.id = log.event_name and event_times.id = log.event_time and source_hosts.id = log.source_host and user_agents.id = log.user_agent and user_names.id = log.user_name and user_keys.id = log.user_key;" database)))

(defun psql-create-table (table &optional db)
  (let ((database (or db "metis")))
    (format t "ct:~A db:~A~%" table database)
    (psql-do-query (format nil "create table if not exists ~A(id serial, value text)" table) database)
    (psql-do-query (format nil "create unique index concurrently if not exists ~A_idx1 on ~A(id)" table table) database)
    (psql-do-query (format nil "create unique index concurrently if not exists ~A_idx2 on ~A(value)" table table) database)))

(defun try-twice (table query)
  (let ((val (or
	      (ignore-errors (get-id-or-insert-psql table query))
		 (get-id-or-insert-psql table query))))
    val))

;;create unique index concurrently if not exists event_names_idx1 on event_names(id)
(fare-memoization:define-memo-function get-id-or-insert-psql (table value)
  (setf *print-circle* nil)
  (psql-do-query (format nil "insert into ~A(value) select '~A' where not exists (select * from ~A where value = '~A')" table value table value))
  (let ((id
	 (flatten
	  (car
	   (car
	    (psql-do-trans
	     (format nil "select id from ~A where value = '~A'" table value)))))))
    ;;(format t "gioip: table:~A value:~A id:~A~%" table value id)
    (if (listp id)
	(car id)
	id)))

(defun get-index-value (table value)
  (let ((one (ignore-errors (get-id-or-insert-psql table value))))
    (unless (typep one 'integer)
      (setf one (get-id-or-insert-psql table value)))
    one))

(defun normalize-insert (event-time user-name user-key event-name user-agent source-host)
  (format t "NI: ~A ~A ~A ~A ~A ~A~%" event-time user-name user-key event-name user-agent source-host)
  (let*
      ((event-time-id (try-twice "event_times" event-time))
       (user-name-id (try-twice "user_names" user-name))
       (user-key-id (try-twice "user_keys" user-key))
       ;;(user-identity-id (try-twice "user_identitys " user-identity))
       (event-name-id (try-twice "event_names" event-name))
       (user-agent-id (try-twice "user_agents" user-agent))
       (source-host-id (try-twice "source_hosts" source-host)))
    (psql-do-query
     (format nil "insert into log(event_time,user_name,user_key,event_name,user_agent,source_host) values ('~A','~A','~A','~A','~A','~A')"
	     event-time-id user-name-id user-key-id event-name-id user-agent-id source-host-id))))

(defun load-file-values ()
  (unless *files*
    (setf *files*
	  (psql-do-query "select value from files" *DB*))
    (mapcar #'(lambda (x)
		(setf (gethash (car x) *h*) t))
	    *files*))
  *h*)
