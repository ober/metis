(in-package :metis)
;;(declaim (optimize (speed 3) (debug 0) (safety 0) (compilation-speed 0)))


(defvar *manard-flow-files* (thread-safe-hash-table))
(ql:quickload :split-sequence :cl-date-time-parser :local-time)
(defvar *mytasks* (list))

(manardb:defmmclass conversation ()
  (
   (interface-id :initarg :interface-id :reader interface-id)
   (srcaddr :initarg :srcaddr :reader srcaddr)
   (srcport :initarg :srcport :reader srcport)
   (dstaddr :initarg :dstaddr :reader dstaddr)
   (dstport :initarg :dstport :reader dstport)
   (protocol :initarg :protocol :reader protocol)
   )
  )

(fare-memoization:define-memo-function get-obj-conversation (interface-id srcaddr srcport dstaddr dstport protocol)
  "Return the object for a given value of klass"
  (let ((obj nil)
	(key-name (format nil "~A-~A-~A-~A-~A-~A" interface-id srcaddr srcport dstaddr dstport protocol)))
    (unless (or (null interface-id) (null srcaddr) (null srcport) (null dstaddr) (null dstport) (null protocol))
      (progn
	(multiple-value-bind (id1 seen1)
	    (gethash 'conversation *metis-fields*)
	  (unless seen1
	    (setf (gethash 'conversation *metis-fields*)
		  (thread-safe-hash-table))))
	(multiple-value-bind (id seen)
	    (gethash key-name (gethash 'conversation *metis-fields*))
	  (if seen
	      (setf obj id)
	      (let (
		    (interface-id-i (get-obj 'metis::interface-id interface-id))
		    (srcaddr-i (get-obj 'metis::srcaddr srcaddr))
		    (srcport-i (get-obj 'metis::srcport srcport))
		    (dstaddr-i (get-obj 'metis::dstaddr dstaddr))
		    (dstport-i (get-obj 'metis::dstport dstport))
		    (protocol-i (get-obj 'metis::protocol protocol))
		    )
		(setf obj (make-instance 'conversation
					 :interface-id interface-id-i
					 :srcaddr srcaddr-i
					 :srcport srcport-i
					 :dstaddr dstaddr-i
					 :dstport dstport-i
					 :protocol protocol-i
					 ))
		(setf (gethash key-name (gethash 'conversation *metis-fields*)) obj))))))
    ;;(format t "get-obj: klass:~A new-value:~A obj:~A~%" klass new-value obj)
    obj))

(manardb:defmmclass date ()
  ((file :initarg :name :reader file)))

(manardb:defmmclass interface-id ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass srcaddr ()
   ((value :initarg :value :accessor value)))

(manardb:defmmclass dstaddr ()
   ((value :initarg :value :accessor value)))

(manardb:defmmclass srcport ()
   ((value :initarg :value :accessor value)))

(manardb:defmmclass dstport ()
   ((value :initarg :value :accessor value)))

(manardb:defmmclass protocol ()
   ((value :initarg :value :accessor value)))

(manardb:defmmclass packets ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass bytez ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass start ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass endf ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass action ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass status ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass flow ()
  ((date :initarg :date :accessor date)
   (conversation :initarg :conversation :accessor conversation)
   ;;(version :initarg version )
   ;;(account_id :initarg account_id)
   (protocol :initarg :protocol :accessor protocol)
   (packets :initarg :packets :accessor packets)
   (bytez :initarg :bytez :accessor bytez)
   (start :initarg :start :accessor start)
   (endf :initarg :endf :accessor endf)
   (action :initarg :action :accessor action)
   (status :initarg :status :accessor status)))

(manardb:defmmclass flow-files ()
  ((file :initarg :file :accessor file)))

;; (defmethod print-object ((flow flow) stream)
;;   (format stream "#<date:~s srcaddr:~s dstaddr:~s srcport:~s dstport:~s>" (date flow) (srcaddr flow) (dstaddr flow) (srcport flow) (dstport flow)))

(defparameter flow-tables '(:dates :versions :account_ids :interface-ids :srcaddrs :dstaddrs :srcports :dstports :protocols :packetss :bytezs :starts :endfs :actions :statuss :flow-files :ips :ports))

(defun bench-vpc-flows-report-sync (dir)
  (let ((path (or dir "~/vpctiny")))
    ;;(defvar benching t)
    (let ((btime (get-internal-real-time))
	  (benching t))
      #+sbcl
      (progn
	(sb-sprof:with-profiling (:report :flat) (vpc-flows-report-sync path)))
      #+lispworks
      (progn
	(hcl:set-up-profiler :package '(metis))
	(hcl:profile (vpc-flows-report-sync path)))
      #+allegro (progn
		  (prof:start-profiler :type :time :count t)
		  (time (vpc-flows-report-sync path))
		  (prof::show-flat-profile))
      #+(or clozure abcl ecl) (time (vpc-flows-report-sync path))
      (let* ((etime (get-internal-real-time))
	     (delta (/ (float (- etime btime)) (float internal-time-units-per-second)))
	     (rows (manardb:count-all-instances 'metis::flow))
	     (convs (manardb:count-all-instances 'metis::conversation))
	     (files (manardb:count-all-instances 'metis::flow-files)))
	;;      (if (and delta rows)
	;;(let ((rps (/ (float rows) (float delta))))
	;;(format t "~%rps:~A delta~A rows:~A files:~A" (/ (float rows) (float delta)) delta (caar rows) (caar files)))))
	(format t "~%delta~A rows:~A files:~A convs:~A" delta (caar rows) (caar files) convs)))))

(defun vpc-flows-report-async (workers path)
  (allocate-vpc-file-hash)
  (let ((workers (parse-integer workers)))
    (setf (pcall:thread-pool-size) workers)
    (walk-ct path #'async-vf-file)
    (mapc
     #'(lambda (x)
	 (if (typep x 'pcall::task)
	     (progn
	       ;;(format t "~%here:~A" (type-of x))
	       (pcall:join x))
	     (format t "~%not ~A" (type-of x))
	     ))
     *mytasks*))
  )


(defun get-unique-conversation ()
  "Return uniqure list of klass objects"
  (manardb:doclass (x 'metis::conversation :fresh-instances nil)
    (with-slots (interface-id srcaddr dstaddr srcport dstport protocol) x
      (format t "int:~A srcaddr:~A dstaddr:~A srcport:~A dstport:~A protocol:~A~%"
	      (get-val interface-id)
	      (get-val srcaddr)
	      (get-val dstaddr)
	      (get-val srcport)
	      (get-val dstport)
	      (get-val protocol)))))

(defun get-by-ip (val)
  (manardb:doclass (x 'metis::conversation :fresh-instances nil)
    (with-slots (interface-id srcaddr dstaddr srcport dstport protocol) x
      (let ((srcaddr-i (get-val srcaddr))
	    (dstaddr-i (get-val dstaddr)))
	(if (or (string-equal val srcaddr-i) (string-equal val dstaddr-i))
		(format t "|~A|~A|~A|~A|~A|~A|~%"
			(get-val interface-id)
			srcaddr-i
			dstaddr-i
			(get-val srcport)
			(get-val dstport)
			(get-val protocol)
			))))))

(defun vpc-flows-report-sync (path)
  (allocate-vpc-file-hash)
  (force-output)
  (unless (null path)
    (walk-ct path #'sync-vf-file)))

(defun async-vf-file (x)
  (push (pcall:pexec
	  (funcall #'process-vf-file x)) *mytasks*))

(defun sync-vf-file (x)
  (process-vf-file x))

;; (defun read-gzip-file (file)
;;   (uiop:run-program
;;    (format nil "zcat ~A" file)
;;    :output :string))


;; (defmacro find-by-field (class field value)
;;   `(let ((myclass (intern (string-upcase class)))
;; 	 (myfield (intern field)))
;;      (mapcar #'(lambda (x)
;; 		 (format t "xxx: ~A ~%" x))
;; 	     (retrieve-from-index 'metis::flow (quote ,field) "53" :all t))))


(defun find-by-srcaddr (value)
  (manardb:doclass (x 'metis::flow :fresh-instances nil)
    (with-slots (
		 interface-id
		 srcaddr
		 dstaddr
		 srcport
		 dstport
		 protocol
		 packets
		 bytez
		 start
		 endf
		 action
		 status
		 ) x
      (if (cl-ppcre:all-matches value srcaddr)
	  (format t "|~A|~A|~A|~A|~A|~A|~A|~A|~A|~A|~A|~A~%"
		  interface-id
		  srcaddr
		  dstaddr
		  srcport
		  dstport
		  protocol
		  packets
		  bytez
		  start
		  endf
		  action
		  status)))))

(defun list-all-vpc ()
  (manardb:doclass (x 'metis::flow :fresh-instances nil)
    (with-slots (
		 interface-id
		 srcaddr
		 dstaddr
		 srcport
		 dstport
		 protocol
		 packets
		 bytez
		 start
		 endf
		 action
		 status
		 ) x
      (format t "|~A|~A|~A|~A|~A|~A|~A|~A|~A|~A|~A|~A~%"
	      interface-id
	      srcaddr
	      dstaddr
	      srcport
	      dstport
	      protocol
	      packets
	      bytez
	      start
	      endf
	      action
	      status))))

;; #+allegro
;; (defun find-by-dstaddr (value)
;;   (mapcar #'(lambda (x)
;; 	      (format t "xxx: ~A ~%" x))
;; 	  (retrieve-from-index 'metis::flow 'dstaddr value :all t)))

;; #+allegro
;; (defun find-by-srcport (value)
;;   (mapcar #'(lambda (x)
;; 	      (format t "xxx: ~A ~%" x))
;; 	  (retrieve-from-index 'metis::flow 'srcport value :all t)))


;; #+allegro
;; (defun find-by-dstport (value)
;;   (mapcar #'(lambda (x)
;; 	      (format t "xxx: ~A ~%" x))
;; 	  (retrieve-from-index 'metis::flow 'date value :all t)))

;; #+allegro
;; (defun find-by-dstport (value)
;;   (mapcar #'(lambda (x)
;; 	      (format t "xxx: ~A ~%" x))
;; 	  (retrieve-from-index 'metis::flow 'dstport value :all t)))


(defun allocate-vpc-file-hash ()
  (if (eql (hash-table-count *manard-flow-files*) 0)
      (init-manardb)
      (mapc
       #'(lambda (x)
	   (setf (gethash (slot-value x 'file) *manard-flow-files*) t))
       (manardb:retrieve-all-instances 'metis::flow-files))))

(defun flows-have-we-seen-this-file (file)
  (let ((name (get-full-filename file)))
    (multiple-value-bind (id seen)
	(gethash name *manard-flow-files*)
      seen)))

(defun flow-mark-file-processed (file)
  (let ((name (get-full-filename file)))
    (setf (gethash name *manard-flow-files*) t)
    (make-instance 'flow-files :file name)))

(defun get-vpc-date-list ()
  "Return uniqure list of events"
  (get-unique-values 'metis::date))

(defun get-vpc-interface-id-list ()
  "Return uniqure list of events"
  (get-unique-values 'metis::interface-id))

(defun get-vpc-srcaddr-list ()
  "Return uniqure list of events"
  (get-unique-values 'metis::srcaddr))

(defun get-vpc-dstaddr-list ()
  "Return uniqure list of events"
  (get-unique-values 'metis::dstaddr))

(defun get-vpc-srcport-list ()
  "Return uniqure list of events"
  (get-unique-values 'metis::srcport))

(defun get-vpc-dstport-list ()
  "Return uniqure list of events"
  (get-unique-values 'metis::dstport))

(defun get-vpc-protocols-list ()
  "Return uniqure list of events"
  (get-unique-values 'metis::protocols))

(defun get-vpc-action-list ()
  "Return uniqure list of events"
  (get-unique-values 'metis::action))

(defun geft-vpc-status-list ()
  "Return uniqure list of events"
  (get-unique-values 'metis::status))

;; (defun flows-get-hash (hash file)
;;   (let ((fullname (get-full-filename file))
;; 	(them (load-file-flow-values)))
;;     (if (gethash fullname them)
;; 	t
;; 	nil)))

;; (defun flow-mark-file-processed (x)
;;   #+allegro (progn
;; 	      (format t "mark-file: ~A" x)
;; 	      (make-instance 'flow_files :name (format nil "~A" (get-full-filename x)))
;; 	      )
;;   #-allegro (progn
;; 	      (let ((fullname (get-full-filename x)))
;; 		(psql-do-query
;; 		 (format nil "insert into flow_files(value) values ('~A')" fullname))
;; 		(setf (gethash (file-namestring x) *h*) t))))

;; (defun process-vf-file (file)
;;   (when (equal (pathname-type file) "gz")
;;     (unless (flows-have-we-seen-this-file file)
;;       (format t "+")
;;       (flow-mark-file-processed file)
;;       (mapcar #'process-vf-line
;; 	      (split-sequence:split-sequence
;; 	       #\linefeed
;; 	       (uiop:run-program (format nil "zcat ~A" file) :output :string))))))

(defun process-vf-file (file)
  (when (equal (pathname-type file) "gz")
    (unless (flows-have-we-seen-this-file file)
      (progn
	(format t "+")
	(flow-mark-file-processed file)
	(gzip-stream:with-open-gzip-file (in file)
	  (let ((i 0)
		(btime (get-internal-real-time)))
	    (loop
	       for line = (read-line in nil nil)
	       while line
	       collect (progn
			 (incf i)
			 (process-vf-line line)))
	    (let* ((etime (get-internal-real-time))
		   (delta (/ (float (- etime btime)) (float internal-time-units-per-second)))
		   (rps (/ (float i) (float delta))))
	      (format t "~%rps:~A rows:~A delta:~A" rps i delta)))))
      (format t "-"))))

(defun process-vf-line (line)
  (let* ((tokens (split-sequence:split-sequence #\Space line))
	 (length (list-length tokens)))
    (if (= 15 length)
	(destructuring-bind (date version account_id interface-id srcaddr dstaddr srcport dstport protocol packets bytez start end action status)
	    tokens
	  (insert-flows date interface-id srcaddr dstaddr srcport dstport protocol packets bytez start end action status)))))

(defun to-epoch (date)
  (local-time:timestamp-to-unix (local-time:universal-to-timestamp (cl-date-time-parser:parse-date-time date))))


(defun insert-flows( date interface-id srcaddr dstaddr srcport dstport protocol packets bytez start endf action status)
  (let (
	(date2 (to-epoch date))
	;;(interface-id-i (get-obj 'metis::interface-id interface-id))
	;;(srcaddr-i (get-obj 'metis::srcaddr srcaddr))
	;;(dstaddr-i (get-obj 'metis::dstaddr dstaddr))
	;;(srcport-i (get-obj 'metis::srcport srcport))
	;;(dstport-i (get-obj 'metis::dstport dstport))
	;;(protocol-i (get-obj 'metis::protocol protocol))
	(conversation-i (get-obj-conversation interface-id srcaddr srcport dstaddr dstport protocol))
	(packets-i (get-obj 'metis::packets packets))
	(bytez-i (get-obj 'metis::bytez bytez))
	(start-i (get-obj 'metis::start start))
	(endf-i (get-obj 'metis::endf endf))
	(action-i (get-obj 'metis::action action))
	(status-i (get-obj 'metis::status status))
	)


    (make-instance 'flow
		   :date date2
		   :conversation conversation-i
		   ;;:interface-id interface-id-i
		   ;;:srcaddr srcaddr-i
		   ;;:dstaddr dstaddr-i
		   ;;:srcport srcport-i
		   ;;:dstport dstport-i
		   ;;:protocol protocol-i
		   :packets packets-i
		   :bytez bytez-i
		   :start start-i
		   :endf endf-i
		   :action action-i
		   :status status-i
		   )
    ))

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

;; (defun recreate-flow-tables(&optional db)
;;   (let ((database (or db "metis")))
;;     (mapcar
;;      #'(lambda (x)
;; 	 (psql-drop-table x database)) flow_tables)
;;     (psql-do-query "drop table if exists raw cascade" database)
;;     (psql-do-query "drop table if exists endpoints cascade" database)
;;     (create-flow-tables)))

;; (defun create-flow-tables (&optional db)
;;   (let ((database (or db "metis")))
;;     (mapcar
;;      #'(lambda (x)
;; 	 (psql-create-table x db)) flow_tables)
;;     (psql-do-query "create table endpoints(id serial unique, host int references ips(id),  port int references ports(id))")
;;     (psql-do-query "create unique index endpoints_idx on endpoints(host,port)")
;;     (psql-do-query "create table if not exists raw(id serial, date integer, interface-id integer, srcaddr integer, dstaddr integer, srcport integer, dstport integer, protocol integer, packets integer, bytez integer, start integer, endf integer, action integer, status integer)" database)
;;     (psql-do-query "create or replace view flows as select dates.value as date, interface-ids.value as interface-id, srcaddrs.value as srcaddr, dstaddrs.value as dstaddr, srcports.value as srcport, dstports.value as dstport, protocols.value as protocol, packetss.value as packets, bytezs.value as bytez, starts.value as start, endfs.value as endf, actions.value as action, statuss.value as status from raw, dates, interface-ids, srcaddrs, dstaddrs, srcports, dstports, protocols, packetss, bytezs, starts, endfs, actions, statuss where dates.id = raw.date and interface-ids.id = raw.interface-id and srcaddrs.id = raw.srcaddr and dstaddrs.id = raw.dstaddr and protocols.id = raw.protocol and packetss.id = raw.packets and bytezs.id = raw.bytez and starts.id = raw.start and endfs.id = raw.endf and actions.id = raw.action and statuss.id = raw.status" database)))
