(in-package :metis)
(ql:quickload :split-sequence)

(defun flows-have-we-seen-this-file (file)
  ;;(format t ".")
  (let ((them (load-file-values "flow_files")))
    (if (gethash (file-namestring file) flow-files)
  	t
	nil)))

(defun vpc-flows-report-async (workers path)
  (let ((workers (parse-integer workers))
	(vpc-flows-reports (or path "~/vpc")))
    ))

(defun async-vf-file (x)
  (push (pcall:pexec
	  (funcall #'process-vf-file x)) *mytasks*))

(defun process-vf-file (x)
  (when (equal (pathname-type x) "gz")
    (unless (have-we-seen-this-file x)
      (db-mark-file-processed x)
      ;;(format t "n")
      ;;(format t "New:~A~%" (file-namestring x))
      (parse-vf-contents x))))

(defun parse-vf-contents (x)
  (format t "+")
  (let* ((records (cdr (elt (read-gzip-file x) 0))))
    (format t "~A" records)))

(defun read-gzip-file (file)
  (uiop:run-program
   (format nil "zcat ~A" file)
   :output :string))

(defun process-vf-file (file)
  (mapcar #'process-vf-line
	  (split-sequence:split-sequence #\linefeed
					 (uiop:run-program (format nil "zcat ~A" file) :output :string))))

(defun process-vf-line (line)
  (let* ((tokens (split-sequence:split-sequence #\Space line))
	 (length (list-length tokens)))
    (if (= 15 length)
	(destructuring-bind (date version account_id interface_id srcaddr dstaddr srcport dstport protocol packets bytez start end action status)
	    tokens
	  (insert-flows date version account_id interface_id srcaddr dstaddr srcport dstport protocol packets bytez start end action status)))))



(defun insert-flows( date version account_id interface_id srcaddr dstaddr srcport dstport protocol packets bytez start endf action status)
  (let*
      (
       (date-id (get-id-or-insert-psql "date" date))
       (version-id (get-id-or-insert-psql "version" version))
       (account_id-id (get-id-or-insert-psql "account_id" account_id))
       (interface_id-id (get-id-or-insert-psql "interface_id" interface_id))
       (srcaddr-id (get-id-or-insert-psql "srcaddr" srcaddr))
       (dstaddr-id (get-id-or-insert-psql "dstaddr" dstaddr))
       (srcport-id (get-id-or-insert-psql "srcport" srcport))
       (dstport-id (get-id-or-insert-psql "dstport" dstport))
       (protocol-id (get-id-or-insert-psql "protocol" protocol))
       (packets-id (get-id-or-insert-psql "packets" packets))
       (bytez-id (get-id-or-insert-psql "bytez" bytez))
       (start-id (get-id-or-insert-psql "start" start))
       (endf-id (get-id-or-insert-psql "endf" endf))
       (action-id (get-id-or-insert-psql "action" action))
       (status-id (get-id-or-insert-psql "status" status)))

    (psql-do-query
     (format nil "insert into raw(date, version, account_id, interface_id, srcaddr, dstaddr, srcport, dstport, protocol, packets, bytez, start, endf, action, status) values ('~A','~A','~A','~A','~A','~A', '~A','~A','~A','~A','~A','~A', '~A','~A','~A')"
	     date-id version-id account_id-id interface_id-id srcaddr-id dstaddr-id srcport-id dstport-id protocol-id packets-id bytez-id start-id endf-id action-id status-id))))

(defun create-flow-tables (&optional db)
  (let ((tables '(:dates :versions :account_ids :interface_ids :srcaddrs :dstaddrs :srcports :dstports :protocols :packetss :bytezs starts :endfs :actions :statuss))
	(database (or db "metis")))
    (mapcar #'(lambda (x)
		(psql-create-table x db)) tables)))
  (psql-do-query "create table if not exists raw(id serial, date integer, version integer, account_id integer, interface_id integer, srcaddr integer, dstaddr integer, srcport integer, dstport integer, protocol integer, packets integer, bytez integer, start integer, endf integer, action integer, status integer" database)
  (psql-do-query "create or replace view flows as select dates.value as date, versions.value as version, account_ids.value as account_id, interface_ids.value as interface_id, srcaddrs.value as srcaddr, dstaddrs.value as dstaddr, srcports.value as srcport, dstports.value as dstport, protocols.value as protocol, packetss.value as packets, bytezs.value as bytez, starts.value as start, endfs.value as endf, actions.value as action, statuss.value as status from raw, dates, versions, account_ids, interface_ids, srcaddrs, dstaddrs, srcports, dstports, protocols, packetss, bytezs, starts, endfs, actions, statuss where dates.id = raw.date and versions.id = raw.version and account_ids.id = raw.account_id and interface_ids.id = raw.interface_id and srcaddrs.id = raw.srcaddr and dstaddrs.id = raw.dstaddr and protocols.id = raw.protocol and packetss.id = raw.packets and bytezs.id = raw.bytez and starts.id = raw.start and endfs.id = raw.endf and actions.id = raw.action and statuss.id = raw.status" database)))
