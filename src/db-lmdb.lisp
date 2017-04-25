(in-package :metis)
(defvar *lmdb-files* (thread-safe-hash-table))
(defvar *metis-fields* (thread-safe-hash-table))
(defvar *metis-counters* (thread-safe-hash-table))
(defvar *metis-need-files* nil)

(defvar ct-fields '(
		    metis::additionalEventData
		    metis::awsRegion
		    metis::errorCode
		    metis::errorMessage
		    ;;metis::eventID
		    metis::eventName
		    metis::eventSource
		    ;;metis::eventTime
		    metis::eventType
		    metis::eventVersion
		    metis::recipientAccountId
		    ;;metis::requestID
		    ;;metis::requestParameters
		    metis::resources
		    ;;metis::responseElements
		    metis::sourceIPAddress
		    metis::userAgent
		    ;;metis::userIdentity
		    metis::userName
		    ))

(defun init-lmdb()
;;  (unless (boundp 'lmdb-use-mmap-dir)
;;    (lmdb:use-mmap-dir (or (uiop:getenv "METIS") "~/ct-lmdb/")))
;;  (if (and (eql (hash-table-count *lmdb-files*) 0) *metis-need-files*)
  ;;      (allocate-file-hash)))
  (format t "Done Initializing~%")
  )

(defclass files ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass additionalEventData ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass awsRegion ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass errorCode ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass errorMessage ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass eventID ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass eventName ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass eventSource ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass eventTime ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass eventType ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass eventVersion ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass recipientAccountId ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass requestID ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass requestParameters ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass resources ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass responseElements ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass sourceIPAddress ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass userAgent ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass userIdentity ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass userName ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(defclass ct ()
  ((addionalEventData :initarg :additionalEventData :accessor additionalEventData)
   (awsRegion :initarg :awsRegion :accessor awsRegion)
   (errorCode :initarg :errorCode :accessor errorCode)
   (errorMessage :initarg :errorMessage :accessor errorMessage)
   (eventID :initarg :eventID :accessor eventID)
   (eventName :initarg :eventName :accessor eventName)
   (eventSource :initarg :eventSource :accessor eventSource)
   (eventTime :initarg :eventTime :accessor eventTime)
   (eventType :initarg :eventType :accessor eventType)
   (eventVersion :initarg :eventVersion :accessor eventVersion)
   (recipientAccountId :initarg :recipientAccountId :accessor recipientAccountId)
   (requestID :initarg :requestID :accessor requestID)
   (requestParameters :initarg :requestParameters :accessor requestParameters)
   (resources :initarg :resources :accessor resources)
   (responseElements :initarg :responseElements :accessor responseElements)
   (sourceIPAddress :initarg :sourceIPAddress :accessor sourceIPAddress)
   (userAgent :initarg :userAgent :accessor userAgent)
   (userIdentity :initarg :userIdentity :accessor userIdentity)
   (userName :initarg :userName :accessor username)
   ))

(defun create-klass-hash (klass)
  (multiple-value-bind (id seen)
      (gethash klass *metis-fields*)
    (unless seen
      (setf (gethash klass *metis-fields*)
	    (thread-safe-hash-table)))))

;;(fare-memoization:define-memo-function get-obj (klass new-value)

(defun get-obj (klass new-value)
  "Return the object for a given value of klass"
  (let ((obj nil))
    (unless (or (null klass) (null new-value))
      (progn
	(create-klass-hash klass)
	(multiple-value-bind (id seen)
	    (gethash new-value (gethash klass *metis-fields*))
	  (if seen
	      (setf obj id)
	      (progn
		(setf obj (make-instance klass :value new-value))
		(setf (gethash new-value (gethash klass *metis-fields*)) obj))))))
    ;;(format t "get-obj: klass:~A new-value:~A obj:~A seen:~A id:~A~%" klass new-value obj seen id))))
    obj))

(defun lmdb-have-we-seen-this-file (file)
  (let ((name (get-filename-hash file)))
    (multiple-value-bind (id seen)
	(gethash name *lmdb-files*)
      seen)))

(defun lmdb-get-files (file)
  (format t "lmdb-get-files:~A~%" file)
  (remove-if-not
   (lambda (x) (string-equal
		(get-filename-hash file)
		(slot-value x :value2)))
   (lmdb-retrieve-all-instances 'metis::files)))

(defun lmdb-mark-file-processed (file)
  (let ((name (get-filename-hash file)))
    (setf (gethash name *lmdb-files*) t)
    (make-instance 'files :value name :idx 1)))

(defun allocate-file-hash ()
  (lmdb-doclass (x 'metis::files :fresh-instances nil)
    (setf (gethash (slot-value x 'value) *lmdb-files*) t)))

(defun allocate-klass-hash (klass)
  (or (hash-table-p (gethash klass *metis-fields*))
      (progn
	(format t "allocating class:~A~%" klass)
	(create-klass-hash klass)
	(lmdb-doclass (x klass :fresh-instances nil)
	  (with-slots (value idx) x
	    (setf (gethash value
			   (gethash klass *metis-fields*)) idx)))
	(setf (gethash klass *metis-counters*)
	      (get-max-id-from-hash
	       (gethash klass *metis-fields*))))))

(defun get-max-id-from-hash (hash)
  ;;(format t "hash: ~A~%" hash)
  (let* ((idxs (alexandria:hash-table-values hash))
	 (max-id 0))
    ;;(format t "idxs: ~A~%" idxs)
    (and idxs
	 (setf max-id
	       (apply #'max
		      (mapcar #'(lambda (x)
				  (if (stringp x) (parse-integer x) x)) idxs))))
    max-id))

(defun init-ct-hashes ()
  (mapc
   #'(lambda (x)
       (allocate-klass-hash x))
   ct-fields))

(defun get-stats ()
  (format t "Totals ct:~A files:~A flows:~A vpc-files:~A ec:~A srcaddr:~A dstaddr:~A srcport:~A dstport:~A protocol:~A~%"
	  (lmdb-count-all-instances 'metis::ct)
	  (lmdb-count-all-instances 'metis::files)
	  (lmdb-count-all-instances 'metis::flow)
	  (lmdb-count-all-instances 'metis::flow-files)
	  (lmdb-count-all-instances 'metis::errorCode)
	  (lmdb-count-all-instances 'metis::srcaddr)
	  (lmdb-count-all-instances 'metis::dstaddr)
	  (lmdb-count-all-instances 'metis::srcport)
	  (lmdb-count-all-instances 'metis::dstport)
	  (lmdb-count-all-instances 'metis::protocol)
	  ))

(defun find-username (userIdentity)
  (let ((a (fetch-value '(:|sessionContext| :|sessionIssuer| :|userName|) userIdentity))
	(b (fetch-value '(:|sessionContext| :|userName|) userIdentity))
	(c (fetch-value '(:|userName|) userIdentity))
	(d (car (last (cl-ppcre:split ":" (fetch-value '(:|arn|) userIdentity)))))
	(e (fetch-value '(:|type|) userIdentity))
	(len (length userIdentity)))
    ;;(format t "a: ~A b:~A c:~A d:~A len:~A username:~A" a b c d len username)
    (or a b c d e)))

;;(cl-ppcre:regex-replace #\newline 'userIdentity " "))))))

(defun get-unique-values (klass)
  "Return unique list of klass objects"
  (lmdb-doclass (x klass :fresh-instances nil)
    (with-slots (value idx) x
      (format t "~%~A: ~A" idx value))))

(defun get-unique-values-list (klass)
  "Return unique list of klass objects"
  (let ((results '()))
    (lmdb-doclass (x klass :fresh-instances nil)
      (with-slots (value idx) x
	(push value results)))
    results))

;; lists
(defun get-ct-files ()
  "Return unique list of ct files"
  (get-unique-values 'metis::files))

(defun get-event-list ()
  "Return unique list of events"
  (get-unique-values 'metis::eventname))

(defun get-errorcode-list ()
  "Return unique list of events"
  (get-unique-values 'metis::errorcode))

(defun get-name-list ()
  "Return unique list of events"
  (get-unique-values 'metis::username))

(defun get-sourceips-list ()
  "Return unique list of events"
  (get-unique-values 'metis::sourceIPAddress))

(defun get-val (obj)
  (if (null obj)
      obj
      (slot-value obj 'value)))

(defun get-obj-by-val (klass val)
  (let ((obj-list nil))
    (lmdb-doclass (x klass :fresh-instances nil)
      (with-slots (value) x
	(if (string-equal val value)
	    (push x obj-list))))
    obj-list))

(defun ct-get-by-klass-value  (klass value &optional inverse)
  (format t "~{~A~}" (ct-get-by-klass-value-real klass value (or inverse nil))))

(defun ct-get-by-klass-value-real (klass value &optional inverse)
  (allocate-klass-hash klass)
  (let ((results '())
	(klass-hash (gethash klass *metis-fields*))
	(slotv nil))
    (multiple-value-bind (id seen)
	(gethash value klass-hash)
      (when (or seen inverse)
	(lmdb-doclass (x 'metis::ct :fresh-instances nil)
	  (with-slots (userName eventTime eventName eventSource sourceIPAddress userAgent errorMessage errorCode userIdentity) x
	    (cond
	      ((equal (find-class klass) (find-class 'metis::userName)) (setf slotv userName))
	      ((equal (find-class klass) (find-class 'metis::eventName)) (setf slotv eventName))
	      ((equal (find-class klass) (find-class 'metis::eventSource)) (setf slotv eventSource))
	      ((equal (find-class klass) (find-class 'metis::sourceIPAddress)) (setf slotv sourceIPAddress))
	      ((equal (find-class klass) (find-class 'metis::errorMessage)) (setf slotv errorMessage))
	      ((equal (find-class klass) (find-class 'metis::errorCode)) (setf slotv errorCode)))
	    (when
		(or
		 (and inverse slotv)
		 (and slotv (ignore-errors (= slotv id))))
	      (push
	       (format t "|~A|~A|~A|~A|~A|~A|~A|~A|~A|~%"
		       (get-val-by-idx 'metis::eventTime eventTime)
		       (get-val-by-idx 'metis::eventName eventName)
		       (get-val-by-idx 'metis::userName userName)
		       (get-val-by-idx 'metis::eventSource eventSource)
		       (get-val-by-idx 'metis::sourceIPAddress sourceIPAddress)
		       (get-val-by-idx 'metis::userAgent userAgent)
		       (get-val-by-idx 'metis::errorMessage errorMessage)
		       (get-val-by-idx 'metis::errorCode errorCode)
		       (find-username (get-val-by-idx 'metis::userIdentity userIdentity)))
	       results))))))
    results))

(defun ct-get-all-errors ()
  (ct-get-by-klass-value 'metis::errorCode nil t))

(defun ct-get-by-name (name)
  (ct-get-by-klass-value 'metis::userName name))

(defun ct-get-by-errorcode (name)
  (ct-get-by-klass-value 'metis::errorCode name))

(defun ct-get-by-errorMessage (name)
  (ct-get-by-klass-value 'metis::errorMessage name))

(defun ct-get-by-eventName (name)
  (ct-get-by-klass-value 'metis::eventName name))

(defun ct-get-by-eventSource (name)
  (ct-get-by-klass-value 'metis::eventSource name))

(defun ct-get-by-sourceIPAddress (name)
  (ct-get-by-klass-value 'metis::sourceIPAddress name))

(defun lmdb-recreate-tables ()
  (format t "lmdb-recreate-tables~%"))


(defun compress-str (str)
  (when str
    (let ((store-me nil))
      (cond
	((consp str) (setf store-me (format nil "~{~A ~}" str)))
	((stringp str) (setf store-me str))
	)
      (if (< (length store-me) 10)
	  (flexi-streams:octets-to-string (thnappy:compress-string store-me))
	  store-me))))

(defun lmdb-normalize-insert (record)
  (destructuring-bind (
		       additionalEventData
		       awsRegion
		       errorCode
		       errorMessage
		       eventID
		       eventName
		       eventSource
		       eventTime
		       eventType
		       eventVersion
		       recipientAccountId
		       requestID
		       requestParameters
		       resources
		       responseElements
		       sourceIPAddress
		       userAgent
		       userIdentity
		       userName
		       )
      record
    (unless (boundp '*lmdb-env*)
      (defvar *lmdb-env* (lmdb:make-environment #P"/home/ubuntu/metis-sbcl.mdb/")))
    (unless (boundp '*lmdb-db*)
      (let ((txn (lmdb:begin-transaction txn)))
	(defvar *lmdb-db* (lmdb:make-database txn "metis"))))

    (let* ((db (lmdb:with-database (*lmdb-db*)))
	   (values (list additionalEventData awsRegion errorCode errorMessage eventID eventName eventSource eventType eventVersion recipientAccountId requestID requestParameters resources responseElements sourceIPAddress userAgent userIdentity userName)))
	   (lmdb:put db (format nil "~A-~A-~A" eventTime eventName eventSource)
		     (flexi-streams:with-output-to-sequence (value) (cl-store:store-object values value))))
    ))



(defun cleanse (var)
  (typecase var
    (null (string var))
    (string var)
    (list (format nil "~{~s = ~s~%~}" var))))

(defun lmdb-get-or-insert-id (table value)
  (format t "lmdb-get-or-insert-id table:~A value:~A~%" table value)
  )

(defun lmdb-drop-table (query)
  (format t "lmdb-drop-table query:~A~%" query)
  )

(defun lmdb-do-query (query)
  (format nil "lmdb-do-query query:~A~%" query)
  )
