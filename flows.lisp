(in-package :metis)
(ql:quickload :split-sequence :cl-date-time-parser :local-time)
(defvar *mytasks* (list))

(defparameter flow_tables '(:dates :versions :account_ids :interface_ids :srcaddrs :dstaddrs :srcports :dstports :protocols :packetss :bytezs :starts :endfs :actions :statuss :flow_files))

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
  (time (vpc-flows-report-async workers path)))

(defun vpc-flows-report-async (workers path)
  (let ((workers (parse-integer workers)))
    (setf (pcall:thread-pool-size) workers)
    (walk-ct path #'async-vf-file)
    ;;    (ignore-errors
      (mapc #'pcall:join *mytasks*)
      ;;)
    )
  )

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
  (let ((fullname (get-full-filename file))
	(them (load-file-flow-values)))
    (if (gethash fullname them)
  	t
	nil)))

(defun flows-get-hash (hash file)
  (let ((fullname (get-full-filename file))
	(them (load-file-flow-values)))
    (if (gethash fullname them)
  	t
	nil)))

(defun flow-mark-file-processed (x)
  (let ((fullname (get-full-filename x)))
    (psql-do-query
     (format nil "insert into flow_files(value) values ('~A')" fullname))
    (setf (gethash (file-namestring x) *h*) t)))

(defun process-vf-file (file)
  (format t "~A~%" file)
  (when (equal (pathname-type file) "gz")
    (unless (flows-have-we-seen-this-file file)
      (format t "+")
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
  (let*
      ((date-id (get-id-or-insert-psql "dates" (to-epoch date)))
       ;;(conversation-id (create-conversation (srcaddr dstaddr sport dstport)))
       ;;(version-id (get-id-or-insert-psql "versions" version))
       ;;(account_id-id (get-id-or-insert-psql "account_ids" account_id))
       (interface_id-id (get-id-or-insert-psql "interface_ids" interface_id))
       (srcaddr-id (get-id-or-insert-psql "srcaddrs" srcaddr))
       (dstaddr-id (get-id-or-insert-psql "dstaddrs" dstaddr))
       (srcport-id (get-id-or-insert-psql "srcports" srcport))
       (dstport-id (get-id-or-insert-psql "dstports" dstport))
       (protocol-id (get-id-or-insert-psql "protocols" protocol))
       (packets-id (get-id-or-insert-psql "packetss" packets))
       (bytez-id (get-id-or-insert-psql "bytezs" bytez))
       (start-id (get-id-or-insert-psql "starts" start))
       (endf-id (get-id-or-insert-psql "endfs" endf))
       (action-id (get-id-or-insert-psql "actions" action))
       (status-id (get-id-or-insert-psql "statuss" status)))
    ;;))
    (psql-do-query
     (format nil "insert into raw(date, interface_id, srcaddr, dstaddr, srcport, dstport, protocol, packets, bytez, start, endf, action, status) values ('~A','~A','~A','~A', '~A','~A','~A','~A','~A','~A', '~A','~A','~A')" date-id interface_id-id srcaddr-id dstaddr-id srcport-id dstport-id protocol-id packets-id bytez-id start-id endf-id action-id status-id))))

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
    (psql-do-query "drop table if exists raw" database)
    (create-flow-tables)))

(defun create-flow-tables (&optional db)
  (let ((database (or db "metis")))
    (mapcar
     #'(lambda (x)
	 (psql-create-table x db)) flow_tables)
  (psql-do-query "create table if not exists raw(id serial, date integer, interface_id integer, srcaddr integer, dstaddr integer, srcport integer, dstport integer, protocol integer, packets integer, bytez integer, start integer, endf integer, action integer, status integer)" database)
  (psql-do-query "create or replace view flows as select dates.value as date, interface_ids.value as interface_id, srcaddrs.value as srcaddr, dstaddrs.value as dstaddr, srcports.value as srcport, dstports.value as dstport, protocols.value as protocol, packetss.value as packets, bytezs.value as bytez, starts.value as start, endfs.value as endf, actions.value as action, statuss.value as status from raw, dates, interface_ids, srcaddrs, dstaddrs, srcports, dstports, protocols, packetss, bytezs, starts, endfs, actions, statuss where dates.id = raw.date and interface_ids.id = raw.interface_id and srcaddrs.id = raw.srcaddr and dstaddrs.id = raw.dstaddr and protocols.id = raw.protocol and packetss.id = raw.packets and bytezs.id = raw.bytez and starts.id = raw.start and endfs.id = raw.endf and actions.id = raw.action and statuss.id = raw.status" database)))
