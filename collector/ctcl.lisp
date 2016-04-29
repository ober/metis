(in-package :ctcl)
(defvar *database* "metis-test")
(defvar *pcallers* 5)
(defvar *files* nil)
(defvar *h* (make-hash-table :test 'equalp))

#+allegro (setf excl:*tenured-bytes-limit* 52428800)
;;#+allegro (setf excl:*global-gc-behavior* :auto)
#+lispworks (setq sys:*stack-overflow-behaviour* nil)
;;(declaim (optimize (speed 3) (safety 0) (space 0)))
(defvar *mytasks* (list))

;; (defun quit (&optional code)
;;   #+allegro (excl:exit code)
;;   #+sbcl (sb-ext::exit)
;;   #+lispworks (quit)
;;   #+clozure (ccl::quit)
;;   #+cmucl (quit)
;;   )

(defun print-cloudtrail-report ()
  (format t "|event-time|user-identity|event-name|user-agent|hostname|~%")
  (format t "|----------|-------------|----------|----------|--------|~%")
  (walk-directory *cloudtrail-reports*
		  (lambda (x)
		    (when (equal (pathname-type x) "gz")
		      (let ((btime (get-internal-real-time)))
			(setq records (cdr (elt (read-json-gzip-file x) 0)))
			;;(enter-metric "" (- (get-universal-time) btime))
			;;(let* ((records (cdr (elt (read-json-gzip-file x) 0))))
			(dolist (x records)
			  (let* ((event-time (cdr-assoc :EVENT-TIME x))
				 (user-identity (cdr-assoc :ACCESS-KEY-ID
							   (cdr-assoc :USER-IDENTITY x)))
				 (event-name (cdr-assoc :EVENT-NAME x))
				 (user-agent (cdr-assoc :USER-AGENT x))
				 (ip (cdr-assoc :SOURCE-+IP+-ADDRESS x))
				 (hostname (get-hostname-by-ip ip)))
			    (format t "|~A|~A|~A|~A|~A|~%" event-time user-identity event-name user-agent (or hostname ip)))))))))

(fare-memoization:define-memo-function get-id-or-insert-psql (table value)
  ;;  (let ((btime (get-internal-real-time)))
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

;;(enter-metric "get-id-or-insert-psql" (- (get-internal-real-time) btime))))

(defun normalize-insert (db event-time user-name user-key event-name user-agent source-host)
  (let ((btime (get-internal-real-time)))
    (let* ((event-time-id (get-id-or-insert-psql "event_times" event-time))
	   (user-name-id (get-id-or-insert-psql "user_names" user-name))
	   (user-key-id (get-id-or-insert-psql "user_keys" user-key))
	   ;;(user-identity-id (get-id-or-insert-psql "user_identitys " user-identity))
	   (event-name-id (get-id-or-insert-psql "event_names" event-name))
	   (user-agent-id (get-id-or-insert-psql "user_agents" user-agent))
	   (source-host-id (get-id-or-insert-psql "source_hosts" source-host)))
      (psql-do-query
       (format nil "insert into log(event_time,user_name,user_key,event_name,user_agent,source_host) values ('~A','~A','~A','~A','~A','~A')"
	       event-time-id user-name-id user-key-id event-name-id user-agent-id source-host-id)))))
;;(enter-metric "normalize-insert" (- (get-internal-real-time) btime))))

(defun load-file-values ()
  (unless *files*
    (progn
      (setf *files* (psql-do-query "select value from files"))
      (mapcar #'(lambda (x)
                  (setf (gethash (car x) *h*) t))
	      *files*)))
  *h*)

(defun have-we-seen-this-file-hash (file)
  (format t ".")
  (let ((them (load-file-values)))
    ;;(maphash #'(lambda (k v) (format t "~a => ~a~%" k v)) them)
    ;;(format t "them: file:~A type:~A val:~A~%" (type-of (file-namestring file)) (type-of them) (gethash (file-namestring file) them))
    ;;(inspect them)
    (if (gethash (file-namestring file) them)
	t
	nil)))

(defun have-we-seen-this-file-preload (file)
  (format t ".")
  (let ((them (load-file-values)))
    ;;(format t "XXX: type of ~A size:~A~%" (type-of them) (length them))
    (if (> (length (member
                    (file-namestring file) them :test
                    (lambda (x y) (string= (flatten y) x)))) 0)
	t
	nil)))

(defun flatten (obj)
  (do* ((result (list obj))
        (node result))
       ((null node) (delete nil result))
    (cond ((consp (car node))
           (when (cdar node) (push (cdar node) (cdr node)))
           (setf (car node) (caar node)))
          (t (setf node (cdr node))))))

(defun psql-drop-table (table)
  (ignore-errors
    (psql-do-query (format nil "drop table if exists ~A cascade" table))))

(defun psql-create-table (table)
  (psql-do-query (format nil "create table if not exists ~A(id serial, value text)" table))
  (psql-do-query (format nil "create unique index ~A_idx1 on ~A(id)" table table))
  (psql-do-query (format nil "create unique index ~A_idx2 on ~A(value)" table table)))

(defun create-tables-psql ()
  (let ((tables '(:event_names :event_times :files :source_hosts :user_agents :user_names :user_keys )))
    (ignore-errors
      (mapcar #'psql-drop-table tables)
      (psql-do-query "drop table if exists log"))
    (mapcar #'psql-create-table tables))
  (psql-do-query "delete from metrics")
  (psql-do-query "create table if not exists log(id serial, event_time integer, user_name integer, user_key integer, event_name integer, user_agent integer, source_host integer)")
  (psql-do-query "create or replace view ct as select event_names.value as event, event_times.value as etime, source_hosts.value as source, user_agents.value as agent, user_names.value as name, user_keys.value as key from event_names,log,event_times,source_hosts,user_agents,user_names,user_keys where event_names.id = log.event_name and event_times.id = log.event_time and source_hosts.id = log.source_host and user_agents.id = log.user_agent and user_names.id = log.user_name and user_keys.id = log.user_key;"))

(defun walk-ct (path fn)
  (walk-directory path fn))

(defun enter-metric (function lisp-time)
  ;;(time (let ((btime (get-internal-real-time))) (sleep 1) (format t "begin:~A end:~A diff:~A~%" btime (get-internal-real-time) (- (get-internal-real-time) btime))))
  #+(or sbcl ecl allegro abcl lispworks) (setq total-time lisp-time)
  #+cmucl (setq total-time (* lisp-time 10))
  #+clozure (setq total-time (* lisp-time 0.001))
  (psql-do-query (format nil "insert into metrics(function, total_time, lisp) values ('~A','~A','~A')" function total-time (lisp-implementation-type))))

(defun have-we-seen-this-file (x)
  (format t ".")
  (if (psql-do-query (format nil "select id from files where value = '~A'" (file-namestring x)))
      t
      nil))

(defun file-has-been-processed (x)
  (psql-do-query
   (format nil "insert into files(value) values ('~A')" (file-namestring x)))
  (setf (gethash (file-namestring x) *h*) t))

(defun file-has-been-processed-preload (x)
  (psql-do-query
   (format nil "insert into files(value) values ('~A')" (file-namestring x))))

(defun sync-ct-file (x)
  (process-ct-file x))

(defun async-ct-file (x)
  (push (pcall:pexec
	 (funcall #'process-ct-file x)) *mytasks*))

(defun process-ct-file (x)
  (let ((btime (get-internal-real-time)))
    (when (equal (pathname-type x) "gz")
      (unless (have-we-seen-this-file-hash x)
	(file-has-been-processed x)
	(format t "New:~A~%" (file-namestring x))
	(parse-ct-contents x)))))
;;(enter-metric "process-ct-file" (- (get-internal-real-time) btime)))))

(defun parse-ct-contents (x)
  (let* ((btime (get-internal-real-time))
	 (records (cdr (elt (read-json-gzip-file x) 0)))
	 (record-size (length records)))
    (dolist (x records)
      (let* ((event-time (cdr-assoc :EVENT-TIME x))
	     (user-identity (cdr-assoc :USER-IDENTITY x))
	     (user-name (cdr-assoc :USER-NAME user-identity))
	     (user-key (cdr-assoc :ACCESS-KEY-ID user-identity))
	     ;;(user-identity (cdr-assoc :ACCESS-KEY-ID (cdr-assoc :USER-IDENTITY x)))
	     (event-name (cdr-assoc :EVENT-NAME x))
	     (etime5 (get-internal-real-time))
	     (user-agent (cdr-assoc :USER-AGENT x))
	     (ip (cdr-assoc :SOURCE-+IP+-ADDRESS x))
	     (hostname (get-hostname-by-ip ip)))
	(normalize-insert nil event-time user-name user-key event-name user-agent (or hostname ip))))))

;;    (let ((etime (get-internal-real-time)))
;;      (enter-metric "parse-ct-contents" (- etime btime))
;;      (enter-metric "parse-ct-per-record" (/ (- etime btime) record-size))))
;;(dump-metrics)
;;  ))

(defun cloudtrail-report-to-psql-sync (path)
  (let ((cloudtrail-reports (or path "~/CT")))
    (walk-ct cloudtrail-reports
	     #'sync-ct-file)))

(defun cloudtrail-report-to-psql-async (workers path)
  (let ((workers (parse-integer workers)))
    (setf (pcall:thread-pool-size) workers)
    (let ((cloudtrail-reports (or path "~/CT")))
      (walk-ct cloudtrail-reports
	       #'async-ct-file))
    (ignore-errors (mapc #'pcall:join *mytasks*))))
