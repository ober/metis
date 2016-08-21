(in-package :metis)
(ql:quickload :split-sequence :cl-date-time-parser :local-time)
(defvar *mytasks* (list))

#+allegro (progn
	    (defclass conversation ()
	      ((name :initarg :name :reader name :index :any-unique))
	      (:metaclass persistent-class))


	    (defclass flow ()
	      ((date :initarg :date :reader date :index)
	       (version :initarg :version )
	       (account_id :initarg :account_id)
	       (interface_id :initarg :interface_id :index)
	       (srcaddr :initarg :srcaddr :index)
	       (dstaddr :initarg :dstaddr :index)
	       (srcport :initarg :srcport :index)
	       (dstport :initarg :dstport :index)
	       (protocol :initarg :protocol)
	       (packets :initarg :packets)
	       (bytez :initarg :bytez)
	       (start :initarg :start)
	       (endf :initarg :endf)
	       (action :initarg :action)
	       (status :initarg :status))
	       (:metaclass persistent-class))

	    (defclass flow_files ()
	      ((name :initarg :name :index :any-unique))
	       (:metaclass persistent-class))

	    ;; (defmethod print-object ((flow flow) stream)
	    ;;   (format stream "#<flow ~s srcport ~s>" (date flow) (srcport flow)))

	    )

(defparameter flow_tables '(:dates :versions :account_ids :interface_ids :srcaddrs :dstaddrs :srcports :dstports :protocols :packetss :bytezs :starts :endfs :actions :statuss :flow_files :ips :ports))

(defun load-file-flow-values ()
  (unless *files*
    (setf *files*
	  (psql-do-query "select value from flow_files"))
    (mapcar #'(lambda (x)
		(setf (gethash (car x) *h*) t))
	    *files*))
  *h*)


(defun bench-vpc-flows-report-async (workers path)
  (recreate-flow-tables)
  #+allegro
  (progn
    (db.ac::open-file-database "flow-db" :if-does-not-exist :create :if-exists :supersede))


  ;;(defvar benching t)
  (let ((btime (get-internal-real-time))
	(benching t))
    #+sbcl
    (progn
      (sb-sprof:with-profiling (:report :flat) (vpc-flows-report-async workers path)))
    #+lispworks
    (progn
      (hcl:set-up-profiler :package '(metis))
      (hcl:profile (vpc-flows-report-async workers path)))
    #+allegro
    (progn
      (prof::with-profiling (:type :space) (vpc-flows-report-async workers path))
      (prof::show-flat-profile))
    #+(or clozure abcl ecl) (time (vpc-flows-report-async workers path))
    (let* ((etime (get-internal-real-time))
	   (delta (/ (float (- etime btime)) (float internal-time-units-per-second)))
	   (files (psql-do-query "select count(*) from flow_files"))
	   (rows (psql-do-query "select count(*) from raw")))
;;      (if (and delta rows)
     ;;(let ((rps (/ (float rows) (float delta))))
      ;;(format t "~%rps:~A delta~A rows:~A files:~A" (/ (float rows) (float delta)) delta (caar rows) (caar files)))))
      (format t "~%delta~A rows:~A files:~A" delta (caar rows) (caar files))))
  #+allegro (db.ac:close-database)
  )

(defun vpc-flows-report-async (workers path)
  (let ((workers (parse-integer workers)))
    (setf (pcall:thread-pool-size) workers)
    (walk-ct path #'async-vf-file)
    ;;        (ignore-errors
    ;;(mapc #'pcall:join *mytasks*)))
    (mapc
     #'(lambda (x)
	 (if (typep x 'pcall::task)
	     (progn
	       (format t "~%here:~A" (type-of x))
	       (pcall:join x))
	     (format t "~%not ~A" (type-of x))
	     ))
     *mytasks*)))

(defun async-vf-file (x)
  (push (pcall:pexec
	  (funcall #'process-vf-file x)) *mytasks*))

(defun read-gzip-file (file)
  (uiop:run-program
   (format nil "zcat ~A" file)
   :output :string))

(defun get-full-filename (x)
  (let* ((split (split-sequence:split-sequence #\/ (directory-namestring x)))
  	 (length (list-length split))
  	 (dir1 (nth (- length 2) split))
  	 (dir2 (nth (- length 3) split)))
    	 (format nil "~A/~A/~A" dir2 dir1 (file-namestring x))))

(defun flows-have-we-seen-this-file (file)
  #+allegro (progn
	      (if (retrieve-from-index 'flow_files 'name (format nil "~A" file))
		  t
		  nil))
  #-allegro (progn
	      (let ((fullname (get-full-filename file))
		    (them (load-file-flow-values)))
		(if (gethash fullname them)
		    t
		    nil))))

(defun flows-get-hash (hash file)
  (let ((fullname (get-full-filename file))
	(them (load-file-flow-values)))
    (if (gethash fullname them)
  	t
	nil)))

(defun flow-mark-file-processed (x)
  #+allegro (progn
	      (make-instance 'flow_files :name (format nil "~A" x))
	      )
  #-allegro (progn
	      (let ((fullname (get-full-filename x)))
		(psql-do-query
		 (format nil "insert into flow_files(value) values ('~A')" fullname))
		(setf (gethash (file-namestring x) *h*) t))))

(defun process-vf-file (file)
  (when (equal (pathname-type file) "gz")
    (unless (flows-have-we-seen-this-file file)
      (format t "+")
      #+allegro (db.ac:commit)
      (flow-mark-file-processed file)
      (mapcar #'process-vf-line
	      (split-sequence:split-sequence
	       #\linefeed
	       (uiop:run-program (format nil "zcat ~A" file) :output :string))))))

(defun process-vf-line (line)
  (let* ((tokens (split-sequence:split-sequence #\Space line))
	 (length (list-length tokens)))
    (if (= 15 length)
	(destructuring-bind (date version account_id interface_id srcaddr dstaddr srcport dstport protocol packets bytez start end action status)
	    tokens
	  (insert-flows date interface_id srcaddr dstaddr srcport dstport protocol packets bytez start end action status)))))

(defun to-epoch (date)
  (local-time:timestamp-to-unix (local-time:universal-to-timestamp (cl-date-time-parser:parse-date-time date))))


(defun insert-flows( date interface_id srcaddr dstaddr srcport dstport protocol packets bytez start endf action status)
  #+allegro
  (progn
    (make-instance 'flow :date date :interface_id interface_id :srcaddr srcaddr :dstaddr dstaddr :srcport srcport :dstport dstport :protocol protocol :packets packets :bytez bytez :start start :endf endf :action action :status status)
    )

  #-allegro
  (progn
    (unless (boundp 'benching)
      (let*
	  ((date-id (get-index-value "dates" (to-epoch date)))
	   ;;(conversation-id (create-conversation (srcaddr dstaddr sport dstport)))
	   ;;(version-id (get-index-value "versions" version))
	   ;;(account_id-id (get-index-value "account_ids" account_id))
	   (interface_id-id (get-index-value "interface_ids" interface_id))
	   (srcaddr-id (get-index-value "srcaddrs" srcaddr))
	   (dstaddr-id (get-index-value "dstaddrs" dstaddr))
	   (srcport-id (get-index-value "srcports" srcport))
	   (dstport-id (get-index-value "dstports" dstport))
	   (protocol-id (get-index-value "protocols" protocol))
	   (packets-id (get-index-value "packetss" packets))
	   (bytez-id (get-index-value "bytezs" bytez))
	   (start-id (get-index-value "starts" start))
	   (endf-id (get-index-value "endfs" endf))
	   (action-id (get-index-value "actions" action))
	   (status-id (get-index-value "statuss" status)))
	(psql-do-query
	 (format nil "insert into raw(date, interface_id, srcaddr, dstaddr, srcport, dstport, protocol, packets, bytez, start, endf, action, status) values ('~A','~A','~A','~A', '~A','~A','~A','~A','~A','~A', '~A','~A','~A')"
		 date-id
		 interface_id-id
		 srcaddr-id
		 dstaddr-id
		 srcport-id
		 dstport-id
		 protocol-id
		 packets-id
		 bytez-id
		 start-id
		 endf-id
		 action-id
		 status-id))))))

;; (fare-memoization:define-memo-function create-conversation(srcaddr dstaddr srcport dstport)
;;   "Create or return the id of the conversation of the passed arguments"
;;   (let ((srcaddr-id (get-id-or-insert-psql "ips" srcaddr))
;; 	(dstaddr-id (get-id-or-insert-psql "ips" dstaddr))
;; 	(sport-id (get-id-or-insert-psql "ports" sport))
;; 	(dport-id (get-id-or-insert-psql "ports" dstport)))

;;     ;;(psql-do-query (format nil "insert into ~A(value)  select '~A' where not exists (select * from ~A where value = '~A')" table value table value))
;;     (psql-do-query (format nil "insert into conversations(srcaddr_id, dstaddr_id, sport_id, dport_id) select '~A' where not exists (select * from conversations where srcaddr_id = '~A' and dstaddr_id = '~A' and sport_id = '~A' and dport_id = '~A')" table value table value))
;;     (let ((id
;; 	   (flatten
;; 	    (car
;; 	     (car
;; 	      (psql-do-query
;; 	       (format nil "select id from ~A where value = '~A'" table value)))))))
;;     ;;(format t "gioip: table:~A value:~A id:~A~%" table value id)
;;       (if (listp id)
;; 	  (car id)
;; 	  id)))
;;   )

(defun recreate-flow-tables(&optional db)
  (let ((database (or db "metis")))
    (mapcar
     #'(lambda (x)
	 (psql-drop-table x database)) flow_tables)
    (psql-do-query "drop table if exists raw cascade" database)
    (psql-do-query "drop table if exists endpoints cascade" database)
    (create-flow-tables)))

(defun create-flow-tables (&optional db)
  (let ((database (or db "metis")))
    (mapcar
     #'(lambda (x)
	 (psql-create-table x db)) flow_tables)
    (psql-do-query "create table endpoints(id serial unique, host int references ips(id),  port int references ports(id))")
    (psql-do-query "create unique index endpoints_idx on endpoints(host,port)")
    (psql-do-query "create table if not exists raw(id serial, date integer, interface_id integer, srcaddr integer, dstaddr integer, srcport integer, dstport integer, protocol integer, packets integer, bytez integer, start integer, endf integer, action integer, status integer)" database)
    (psql-do-query "create or replace view flows as select dates.value as date, interface_ids.value as interface_id, srcaddrs.value as srcaddr, dstaddrs.value as dstaddr, srcports.value as srcport, dstports.value as dstport, protocols.value as protocol, packetss.value as packets, bytezs.value as bytez, starts.value as start, endfs.value as endf, actions.value as action, statuss.value as status from raw, dates, interface_ids, srcaddrs, dstaddrs, srcports, dstports, protocols, packetss, bytezs, starts, endfs, actions, statuss where dates.id = raw.date and interface_ids.id = raw.interface_id and srcaddrs.id = raw.srcaddr and dstaddrs.id = raw.dstaddr and protocols.id = raw.protocol and packetss.id = raw.packets and bytezs.id = raw.bytez and starts.id = raw.start and endfs.id = raw.endf and actions.id = raw.action and statuss.id = raw.status" database)))
