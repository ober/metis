(in-package :metis)

;;(defparameter *q* (make-instance 'queue))
(defvar *h* (make-hash-table :test 'equalp))
(defvar *DB* nil)
(defvar *pcallers* 5)
(defvar dbtype "postgres")


(defun initialize-hashes ()
  (defparameter to-db (make-instance 'queue))
  (defvar event_names (load-normalized-values "event_names"))
  (defvar event_times (load-normalized-values "event_times"))
  (defvar files (load-normalized-values "files"))
  (defvar source_hosts (load-normalized-values "source_hosts"))
  (defvar user_agents (load-normalized-values "user_agents"))
  (defvar user_names (load-normalized-values "user_names"))
  (defvar user_keys (load-normalized-values "user_keys")))

(defun mark-file-processed (x)
  (format t "~A~%" (get-id-or-update-hash (file-namestring x))))

(defun psql-do-query (query &optional db)
  (let ((database (or db "metis"))
	(user-name "metis")
	(password "metis")
	(host "localhost"))
    (postmodern:with-connection
	`(,database ,user-name ,password ,host :pooled-p t)
      (postmodern:query query))))

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

;;create unique index concurrently if not exists event_names_idx1 on event_names(id)
(fare-memoization:define-memo-function get-id-or-insert-psql (table value)
  (setf *print-circle* nil)
  (psql-do-query (format nil "insert into ~A(value) select '~A' where not exists (select * from ~A where value = '~A')" table value table value))
  (let ((id
	 (flatten
	  (car
	   (car
	    (psql-do-query
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
  (let*
      ((event-time-id (get-id-or-update-hash event_times event-time))
       (user-name-id (get-id-or-update-hash user_names user-name))
       (user-key-id (get-id-or-update-hash user_keys user-key))
       ;;(user-identity-id (get-id-or-update-hash user_identitys user-identity))
       (event-name-id (get-id-or-update-hash event_names event-name))
       (user-agent-id (get-id-or-update-hash user_agents user-agent))
       (source-host-id (get-id-or-update-hash source_hosts source-host)))
    (enqueue (format nil
		     "insert into log(event_time,user_name,user_key,event_name,user_agent,source_host) values ('~A','~A','~A','~A','~A','~A')"
		     event-time-id
		     user-name-id
		     user-key-id
		     event-name-id
		     user-agent-id
		     source-host-id) to-db)))

;; (defun normalize-insert (event-time user-name user-key event-name user-agent source-host)
;;   (let*
;;       ((event-time-id (get-id-or-insert-psql "event_times" event-time))
;;        (user-name-id (get-id-or-insert-psql "user_names" user-name))
;;        (user-key-id (get-id-or-insert-psql "user_keys" user-key))
;;        ;;(user-identity-id (get-id-or-insert-psql "user_identitys " user-identity))
;;        (event-name-id (get-id-or-insert-psql "event_names" event-name))
;;        (user-agent-id (get-id-or-insert-psql "user_agents" user-agent))
;;        (source-host-id (get-id-or-insert-psql "source_hosts" source-host)))
;;     (psql-do-query
;;      (format nil "insert into log(event_time,user_name,user_key,event_name,user_agent,source_host) values ('~A','~A','~A','~A','~A','~A')"
;; 	     event-time-id user-name-id user-key-id event-name-id user-agent-id source-host-id))))

(defun load-file-values ()
  (unless *files*
    (setf *files*
	  (psql-do-query "select value from files" *DB*))
    (mapcar #'(lambda (x)
		(setf (gethash (car x) *h*) t))
	    *files*))
  *h*)

(defun load-normalized-values (table)
  (let* ((values-hash (make-hash-table :test 'equalp ))
	 (query (format nil "select id, value from ~A order by id" table))
	 (values (psql-do-query query)))
    (mapc
     #'(lambda (x)
	 (destructuring-bind (id value)
	     x
	   (setf (gethash value values-hash) id)))
     values)
    values-hash))

(defun hash-max-key (hash)
  (reduce #'max (alexandria:hash-table-values hash)))

(defun get-id-or-update-hash (hash value)
  "Look up the id value in a hash.
  If found, return it.
  Otherwise add it to hash and +1 *maxima*::hash::max-id"
  (multiple-value-bind (id found)
      (gethash value hash)
    (if found
	id
	(let ((new-id (+ (hash-max-key hash) 1)))
	  (setf (gethash value hash) new-id)
	  new-id))))

(defun sync-hash-to-table (table hash)
  ;; just sync.
  (let* ((query (format nil "select max(id) from ~A" table))
	 (max-id (car (flatten (psql-do-query query))))
	 (max-hash-value (hash-max-key hash)))
    ;;(format t "query:~A max-id:~A max-hash-value:~A~%" query max-id max-hash-value)))
    (if (> max-hash-value max-id)
    	(loop for x from (+ max-id 1) to max-hash-value
	   do (progn
		(let ((query (format nil "insert into ~A(value) values(\'~A\')" table (gethash x hash))))
		  (psql-do-query query)))))))
