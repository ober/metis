(in-package :ctcl)

(fare-memoization:define-memo-function get-hostname-by-ip (ip)
  (let ((benching (get-env "BENCHING")))
    (if (string= benching "yes")
	(progn 
	  (format t "Benching is set to yes. Disabling dns lookups!:~A~%" benching)
	  "bogus.host.com")
	(progn
	  #+allegro
	  (socket:ipaddr-to-hostname ip)
	  #+sbcl
	  (ignore-errors (sb-bsd-sockets:host-ent-name
			  (sb-bsd-sockets:get-host-by-address
			   (sb-bsd-sockets:make-inet-address ip))))
	  #+lispworks
	  (comm:get-host-entry ip :fields '(:name))
	  #+clozure
	  (ignore-errors (ccl:ipaddr-to-hostname (ccl:dotted-to-ipaddr ip)))))))

(defun read-json-gzip-file (file)
  (with-input-from-string
      (s
       (uiop:run-program
	(format nil "zcat ~A" file)
	:output :string))
    (cl-json:decode-json s)))

;; (defun read-json-gzip-file (file)
;;   (with-input-from-string
;;       (s (apply #'concatenate 'string
;; 		(gzip-stream:with-open-gzip-file (in file)
;; 		  (loop for l = (read-line in nil nil)
;; 		     while l collect l))))
;;     (cl-json:decode-json s)))

(defun cdr-assoc (item a-list &rest keys)
  (cdr (apply #'assoc item a-list keys)))

(define-setf-expander cdr-assoc (item a-list &rest keys)
  (let ((item-var (gensym))
        (a-list-var (gensym))
        (store-var (gensym)))
    (values
     (list item-var a-list-var)
     (list item a-list)
     (list store-var)
     `(let ((a-cons (assoc ,item-var ,a-list-var ,@ keys)))
        (if a-cons
            (setf (cdr a-cons) ,store-var)
            (setf ,a-list (acons ,item-var ,store-var ,a-list-var)))
        ,store-var)
     `(cdr (assoc ,item-var ,a-list-var ,@ keys)))))

(defclass queue ()
  ((list :initform nil)
   (tail :initform nil)))

(defmethod print-object ((queue queue) stream)
  (print-unreadable-object (queue stream :type t)
    (with-slots (list tail) queue
      (cond ((cddddr list)
	     ;; at least five elements, so print ellipsis
	     (format stream "(~{~S ~}... ~S)"
		     (subseq list 0 3) (first tail)))
	    ;; otherwise print whole list
	    (t (format stream "~:S" list))))))

(defmethod dequeue ((queue queue))
  (with-slots (list) queue
    (pop list)))

(defmethod enqueue (new-item (queue queue))
  (with-slots (list tail) queue
    (let ((new-tail (list new-item)))
      (cond ((null list) (setf list new-tail))
	    (t (setf (cdr tail) new-tail)))
      (setf tail new-tail)))
  queue)

(defun get-env (var)
  #+sbcl
  (sb-posix:getenv var)
  #+clozure
  (ccl:getenv var)
  #+allegro
  (sys:getenv var)
  #+lispworks
  (lw:environment-variable var))


