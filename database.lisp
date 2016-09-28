(in-package :metis)

;;(defparameter *q* (make-instance 'queue))
(defvar *h* (make-hash-table :test 'equalp))
(defvar *DB* nil)
(defvar *pcallers* 5)
(defvar dbtype "postgres")
(defvar syncing nil)
(defvar maxima (make-hash-table :test 'equalp))

(defun initialize-hashes ()
  (format t "initializing hashes~%")
  (defparameter to-db (pcall-queue:make-queue))
  (defvar event_names (load-normalized-values "event_names"))
  (defvar event_times (load-normalized-values "event_times"))
  (defvar files (load-normalized-values "files"))
  (defvar source_hosts (load-normalized-values "source_hosts"))
  (defvar user_agents (load-normalized-values "user_agents"))
  (defvar user_names (load-normalized-values "user_names"))
  (defvar user_keys (load-normalized-values "user_keys"))
  (format t "done with hashes~%")
  )

(defun mark-file-processed (x)
  ;;(format t "q:~A~%" (pcall-queue:queue-length to-db))
  (get-id-or-update-hash files (file-namestring x) "files"))

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
      ((event-time-id (get-id-or-update-hash event_times event-time "event-time"))
       (user-name-id (get-id-or-update-hash user_names user-name "user-name"))
       (user-key-id (get-id-or-update-hash user_keys user-key "user-key"))
       ;;(user-identity-id (get-id-or-update-hash user_identitys user-identity "user-identity"))
       (event-name-id (get-id-or-update-hash event_names event-name "event-name"))
       (user-agent-id (get-id-or-update-hash user_agents user-agent "user-agent"))
       (source-host-id (get-id-or-update-hash source_hosts source-host "source-host")))
    ;;(format t "q:~A~%" (queue-length to-db))
    (pcall-queue:queue-push (format nil
		     "~A~C~A~C~A~C~A~C~A~C~A"
		     event-time-id #\tab
		     user-name-id #\tab
		     user-key-id #\tab
		     event-name-id #\tab
		     user-agent-id #\tab
		     source-host-id) to-db)))

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
  (let ((values (alexandria:hash-table-values hash)))
    (if (null values)
	0
      (reduce #'max values))))


(defun block-if-syncing ()
  (loop while syncing
     do (progn
	  (format t "s")
	  (sleep 1))))


(defun get-id-or-update-hash (hash value max)
  "Look up the id value in a hash.
  If found, return it.
  Otherwise add it to hash and +1 *maxima*::hash::max-id"
  (if (null value)
      (setf value "nil")) ;; sql needs a string.

  ;;(format t "~%X max:~A value:~A type:~A null?~A~%" max value (type-of value) (null value))
  (block-if-syncing)

  (multiple-value-bind (max-value found) (gethash max maxima)
    (if (null found)
	(setf (gethash max maxima) (hash-max-key hash))))

  (multiple-value-bind (id found)
      (gethash value hash)
    (if found
	id
	(let ((new-id (+ (gethash max maxima) 1)))
	  (setf (gethash value hash) new-id)
	  (setf (gethash max maxima) new-id)
	  ;;		  (format t "~%X: value:~A max:~A type:~A new-id:~A~%" value max (type-of value) new-id)
	  new-id))))

(defun periodic-sync (q-len)
  (if (null syncing)
      (progn
	(setf syncing t)
	(format t "Sync limit of ~A hit." q-len)
	(sync-world)
	(setf syncing nil))
      (format t "sync already running...~%")))

(defun sync-hash-to-table (table hash)
  ;; just sync.
  (let* ((query (format nil "select max(id) from ~A" table))
	 (max-id (car (flatten (psql-do-query query))))
	 (max-hash-value (hash-max-key hash))
	 (hash-alist (alexandria:hash-table-alist hash))
	 )
    (unless (integerp max-id) (setf max-id 0))
    (if (> max-hash-value max-id)
	(format t "~%syncing ~A: ~A entries...~%" table (- max-hash-value max-id))
    	(loop for x from (+ max-id 1) to max-hash-value
	   do (progn
		(let* ((value (car (rassoc x hash-alist)))
		       (query (format nil "insert into ~A(value) values(\'~A\')" table value)))
		  (unless (null value)
		    (progn
		      ;;(format t "sql:~A~%" query)
		      (psql-do-query query)))))))))

(defun sync-world ()
  (format t "~%Syncing world~%")
  (sync-hash-to-table "event_names" event_names)
  (sync-hash-to-table "event_times" event_times)
  (sync-hash-to-table "files" files)
  (sync-hash-to-table "source_hosts" source_hosts)
  (sync-hash-to-table "user_agents" user_agents)
  (sync-hash-to-table "user_names" user_names)
  (sync-hash-to-table "user_keys" user_keys)
  (time (emit-drain-file to-db))
  (format t "~%World sync complete~%"))

(defun drain-queue (queue)
  (format t "Draining ~A log entries into postgres...~%" (pcall-queue:queue-length queue))
  (loop while (> (pcall-queue:queue-length queue) 0)
     do (progn
	  (let ((query (format nil "insert into log(event_time,user_name,user_key,event_name,user_agent,source_host) values ~A;" (pcall-queue:queue-pop queue))))
;;;	    (format t "%%")
	    (psql-do-query query))))
    (format t "Draining complete.~%"))

(defun emit-drain-file (queue)
  "Dump the queue to a csv file for import to postgres"
  (format t "Draining ~A log entries into postgres...~%" (pcall-queue:queue-length queue))
  (with-open-file (drain "/tmp/loadme.txt" :direction :output :if-exists :supersede)
    (format drain "\COPY log(event_time, user_name, user_key, event_name, user_agent, source_host) FROM STDIN;~%")
    (loop while (not (pcall-queue:queue-empty-p queue))
       do (progn
	    (format drain "~A~%" (pcall-queue:queue-pop queue))))
    (format drain "\\.~%"))
  (uiop:run-program (format nil "cat /tmp/loadme.txt|psql -U metis -d metis"))
  (format t "Draining complete.~%"))
