(in-package :metis)
;;(declaim (optimize (speed 3) (debug 0) (safety 0) (compilation-speed 0)))
(defvar *manard-files* (thread-safe-hash-table))
(defvar *metis-fields* (thread-safe-hash-table))
(defvar *metis-need-files* nil)


(defvar ct-fields '(
		    metis::additionalEventData
		    metis::awsRegion
		    metis::errorCode
		    metis::errorMessage
		    metis::eventID
		    metis::eventName
		    metis::eventSource
		    metis::eventTime
		    metis::eventType
		    metis::eventVersion
		    metis::recipientAccountId
		    metis::requestID
		    metis::requestParameters
		    metis::resources
		    ;;metis::responseElements
		    metis::sourceIPAddress
		    metis::userAgent
		    ;;metis::userIdentity
		    metis::userName
		    ))

(defun init-manardb()
  (unless (boundp 'manardb:use-mmap-dir)
    (manardb:use-mmap-dir "~/ct-manardb/"))
  (if (and (eql (hash-table-count *manard-files*) 0) *metis-need-files*)
      (allocate-file-hash)))

(manardb:defmmclass files ()
  ((value :type STRING :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass additionalEventData ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass awsRegion ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass errorCode ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass errorMessage ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass eventID ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass eventName ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass eventSource ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass eventTime ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass eventType ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass eventVersion ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass recipientAccountId ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass requestID ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass requestParameters ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass resources ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass responseElements ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass sourceIPAddress ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass userAgent ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass userIdentity ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass userName ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass ct ()
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

(fare-memoization:define-memo-function get-obj (klass new-value)
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

(defun manardb-have-we-seen-this-file (file)
  (let ((name (get-filename-hash file)))
    (multiple-value-bind (id seen)
	(gethash name *manard-files*)
      seen)))

(defun manardb-get-files (file)
  (remove-if-not
   (lambda (x) (string-equal (get-filename-hash file) (slot-value x 'value)))
   (manardb:retrieve-all-instances 'metis::files)))

(defun manardb-mark-file-processed (file)
  (let ((name (get-filename-hash file)))
    (setf (gethash name *manard-files*) t)
    (make-instance 'files :value name :idx 1)))

(defun allocate-file-hash ()
  (manardb:doclass (x 'metis::files :fresh-instances nil)
    (setf (gethash (slot-value x 'value) *manard-files*) t)))

(defun allocate-klass-hash (klass)
  (or (hash-table-p (gethash klass *metis-fields*))
      (progn
	(format t "allocating class:~A~%" klass)
	(create-klass-hash klass)
	(manardb:doclass (x klass :fresh-instances nil)
	  (with-slots (value idx) x
	    (setf (gethash value (gethash klass *metis-fields*)) idx))))))

(defun init-ct-hashes ()
  (mapc
   #'(lambda (x)
       (allocate-klass-hash x))
   ct-fields))

(defun get-stats ()
  (format t "Totals ct:~A files:~A flows:~A vpc-files:~A ec:~A srcaddr:~A dstaddr:~A srcport:~A dstport:~A protocol:~A~%"
	  (manardb:count-all-instances 'metis::ct)
	  (manardb:count-all-instances 'metis::files)
	  (manardb:count-all-instances 'metis::flow)
	  (manardb:count-all-instances 'metis::flow-files)
	  (manardb:count-all-instances 'metis::errorCode)
	  (manardb:count-all-instances 'metis::srcaddr)
	  (manardb:count-all-instances 'metis::dstaddr)
	  (manardb:count-all-instances 'metis::srcport)
	  (manardb:count-all-instances 'metis::dstport)
	  (manardb:count-all-instances 'metis::protocol)
	  ))

(fare-memoization:define-memo-function  find-username (userIdentity)
  (let ((a (fetch-value '(:|sessionContext| :|sessionIssuer| :|userName|) userIdentity))
	(b (fetch-value '(:|sessionContext| :|userName|) userIdentity))
	(c (fetch-value '(:|userName|) userIdentity))
	(d (fetch-value '(:|type|) userIdentity))
	(len (length userIdentity)))
    ;;(format t "a: ~A b:~A c:~A d:~A len:~A username:~A" a b c d len username)
    (or a b c d)))


;;(cl-ppcre:regex-replace #\newline 'userIdentity " "))))))

(defun get-unique-values (klass)
  "Return unique list of klass objects"
  (manardb:doclass (x klass :fresh-instances nil)
    (with-slots (value idx) x
      (format t "~%~A: ~A" idx value))))
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
    ;;(let ((obj nil))
    (manardb:doclass (x klass :fresh-instances nil)
      (with-slots (value) x
	(if (string-equal val value)
	    ;;(setf obj x))))
	    (push x obj-list))))
    ;;obj))
    obj-list))


(defun ct-get-by-klass-value (klass value)
  (allocate-klass-hash klass)
  (let* ((klass-hash (gethash klass *metis-fields*))
	 (slotv nil))
    (multiple-value-bind (id seen)
	(gethash value klass-hash)
      (if seen
	  (progn
	    (manardb:doclass (x 'metis::ct :fresh-instances nil)
	      (with-slots (userName eventTime eventName eventSource sourceIPAddress userAgent errorMessage errorCode userIdentity) x
		(cond
		  ((equal (find-class klass) (find-class 'metis::userName)) (setf slotv userName))
		  ((equal (find-class klass) (find-class 'metis::eventName)) (setf slotv eventName))
		  ((equal (find-class klass) (find-class 'metis::eventSource)) (setf slotv eventSource))
		  ((equal (find-class klass) (find-class 'metis::sourceIPAddress)) (setf slotv sourceIPAddress))
		  ((equal (find-class klass) (find-class 'metis::errorMessage)) (setf slotv errorMessage))
		  ((equal (find-class klass) (find-class 'metis::errorCode)) (setf slotv errorCode)))
		(and (= slotv id)
		     (format t "|~A|~A|~A|~A|~A|~A|~A|~A|~%"
			     (get-val-by-idx 'metis::eventTime eventTime)
			     (get-val-by-idx 'metis::eventName eventName)
			     (get-val-by-idx 'metis::userName userName)
			     (get-val-by-idx 'metis::eventSource eventSource)
			     (get-val-by-idx 'metis::sourceIPAddress sourceIPAddress)
			     (get-val-by-idx 'metis::userAgent userAgent)
			     (get-val-by-idx 'metis::errorMessage errorMessage)
			     (get-val-by-idx 'metis::errorCode errorCode))))))
	  (format t "Error: have not seen class: ~A value:~A~%" klass value)))))


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

(defun manardb-recreate-tables ()
  (format t "manardb-recreate-tables~%"))

(defun manardb-normalize-insert-2 (record)
  ;;manardb-nomalize-insert (NIL us-west-1 NIL NIL 216e957f-230e-42ea-bfc7-e0d07d321a8b DescribeDBInstances rds.amazonaws.com 2015-08-07T19:04:52Z AwsApiCall 1.03 224108527019 2f1d4165-3d37-11e5-aae4-c1965b0823e9 NIL NIL NIL bogus.example.com signin.amazonaws.com (invokedBy signin.amazonaws.com sessionContext (attributes (creationDate 2015-08-07T11:17:07Z mfaAuthenticated true)) userName meylor accessKeyId ASIAIOHLZS2V2QON52LA accountId 224108527019 arn arn:aws:iam::224108527019:user/meylor principalId AIDAJVKKNU5BSTZIOF3EU type IAMUser) meylor)
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
    (make-instance 'ct
		   :additionalEventData additionalEventData
		   :awsRegion awsRegion
		   :errorCode errorCode
		   :errorMessage errorMessage
		   :eventID eventID
		   :eventName eventName
		   :eventSource eventSource
		   :eventTime eventTime
		   :eventType eventType
		   :eventVersion eventVersion
		   :recipientAccountId recipientAccountId
		   :requestID requestID
		   :requestParameters requestParameters
		   :resources resources
		   :responseElements responseElements
		   :sourceIPAddress sourceIPAddress
		   :userAgent userAgent
		   :userIdentity userIdentity
		   :userName userName
		   )
    ))


(defun manardb-normalize-insert (record)
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
    (let ((additionalEventData-i (get-idx 'metis::additionalEventData additionalEventData))
	  (awsRegion-i (get-idx 'metis::awsRegion awsRegion))
	  (errorCode-i (get-idx 'metis::errorCode errorCode))
	  (errorMessage-i (get-idx 'metis::errorMessage errorMessage))
	  (eventID-i (get-idx 'metis::eventID eventID))
	  (eventName-i (get-idx 'metis::eventName eventName))
	  (eventSource-i (get-idx 'metis::eventSource eventSource))
	  (eventTime-i (get-idx 'metis::eventTime eventTime))
	  (eventType-i (get-idx 'metis::eventType eventType))
	  (eventVersion-i (get-idx 'metis::eventVersion eventVersion))
	  (recipientAccountId-i (get-idx 'metis::recipientAccountId recipientAccountId))
	  (requestID-i (get-idx 'metis::requestID requestID))
	  (requestParameters-i (get-idx 'metis::requestParameters requestParameters))
	  (resources-i (get-idx 'metis::resources resources))
	  (responseElements-i (get-idx 'metis::responseElements responseElements))
	  (sourceIPAddress-i (get-idx 'metis::sourceIPAddress sourceIPAddress))
	  (userAgent-i (get-idx 'metis::userAgent userAgent))
	  (userIdentity-i (get-idx 'metis::userIdentity userIdentity))
	  (userName-i (get-idx 'metis::userName (or userName (find-username userIdentity)))))
      (make-instance 'ct
		     :additionalEventData additionalEventData-i
		     :awsRegion awsRegion-i
		     :errorCode errorCode-i
		     :errorMessage errorMessage-i
		     :eventID eventID-i
		     :eventName eventName-i
		     :eventSource eventSource-i
		     :eventTime eventTime-i
		     :eventType eventType-i
		     :eventVersion eventVersion-i
		     :recipientAccountId recipientAccountId-i
		     :requestID requestID-i
		     :requestParameters requestParameters-i
		     :resources resources-i
		     :responseElements responseElements-i
		     :sourceIPAddress sourceIPAddress-i
		     :userAgent userAgent-i
		     :userIdentity userIdentity-i
		     :userName userName-i
		     ))))

(defun cleanse (var)
  (typecase var
    (null (string var))
    (string var)
    (list (format nil "~{~s = ~s~%~}" var))))

(defun manardb-get-or-insert-id (table value)
  (format t "manard-get-or-insert-id table:~A value:~A~%" table value)
  )

(defun manardb-drop-table (query)
  (format t "manardb-drop-table query:~A~%" query)
  )

(defun manardb-do-query (query)
  (format nil "manardb-do-query query:~A~%" query)
  )
