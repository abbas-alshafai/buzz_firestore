import 'package:buzz_firestore/src/constants/Constants.dart';
import 'package:buzz_firestore/src/constants/error_constants.dart';
import 'package:buzz_logger/models/log.dart';
import 'package:buzz_logger/service/logger.dart';
import 'package:buzz_repo/interfaces/db_typedef.dart';
import 'package:buzz_repo/interfaces/json_object.dart';
import 'package:buzz_repo/interfaces/remote_repo.dart';
import 'package:buzz_repo/models/ids.dart';
import 'package:buzz_result/models/result.dart';
import 'package:buzz_utils/buzz_utils.dart';
import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:cloud_functions/cloud_functions.dart';

final _firestore = FirebaseFirestore.instance;

class FirestoreRepo<T> implements RemoteRepo {
  final FirebaseFirestore firestore = _firestore;
  final Map<String, String>? paths;

  FirestoreRepo({
    this.paths,
  });

  FromJsonFunc? _fromJsonFunc;
  CollectionReference? _collectionPath;


  // TODO add pagination and order variables here so they would be available for all repos

  @override
  Future init() async {}

  @override
  RemoteRepo ofTable(
    String tableName, {
    final bool? hasPrePath,
  }) {
    if (!(hasPrePath == true)) {
      _collectionPath = firestore.collection(tableName);
      return this;
    }

    String _path = '';
    if (paths != null) {
      for (final key in paths!.keys) {
        _path = '$_path$key/${paths![key]}/';
      }
    }

    _collectionPath = firestore.collection('$_path$tableName');
    return this;
  }

  Result<T> _getErrorLog<T>({
    required final String msg,
    final StackTrace? stacktrace,
  }) =>
      Result<T>.error(
        log: Log(
          stacktrace: stacktrace,
          logLevel: LogLevel.error,
          translationKey: Err.serverErr,
          msg: _buildMessage(msg),
        ),
      );

  String _buildMessage(final String msg) =>
      '(path ${_collectionPath?.path}): $msg';

  /*
  return the id of the created object
   */
  @override
  Future<Result<String>> add<T>(JsonObject dto) async {
    try {
      Logger.log(
        log: Log.i(_buildMessage(
            'Attempting to add ${dto.runtimeType.toString()} object')),
      );

      final map = dto.toJson();
      DocumentReference result = await _collectionPath!.add(map);
      return StringUtils.instance.isNotBlank(result.id)
          ? Result.success(obj: result.id)
          : _getErrorLog(
              stacktrace: StackTrace.current,
              msg: 'Received an empty result when adding an object',
            );
    } catch (e, stacktrace) {
      return _getErrorLog(
        msg: e.toString(),
        stacktrace: stacktrace,
      );
    }
  }

  @override
  Future<Result<T>> update<T>(JsonObject<dynamic> dto) async {
    final id = dto.ids.id;
    assert(StringUtils.instance.isNotBlank(id));
    try {
      Logger.log(
        log: Log.i(_buildMessage(
            '${_collectionPath?.path}: Attempting to update object $id')),
      );

      final map = dto.toJson();
      await _collectionPath!.doc(id).update(map);

      return Result.success(obj: (dto as T));
    } catch (e, stacktrace) {
      return _getErrorLog(
        msg: e.toString(),
        stacktrace: stacktrace,
      );
    }
  }

  @override
  Future<Result<T>> addById<T>(JsonObject dto) async {
    try {
      assert(dto.ids.hasIDs);

      Logger.log(
          log: Log.i(
              _buildMessage('Attempting to add ${dto.runtimeType} object, '
                  'with id ${dto.ids.id}')));

      await _collectionPath!.doc(dto.ids.id).set(dto.toJson());

      return Result.success(obj: dto as T);
    } catch (e, stacktrace) {
      return _getErrorLog(
        msg: e.toString(),
        stacktrace: stacktrace,
      );
    }
  }

  @override
  Future<Result<T>> get<T>(IDs ids) async {
    try {
      assert(StringUtils.instance.isNotBlank(ids.id));

      Logger.log(
        log: Log.i(
          _buildMessage('Attempting to get a record with id ${ids.id}'),
        ),
      );
      DocumentSnapshot result = await _collectionPath!.doc(ids.id).get();
      return _buildResult(result);
    } catch (e, stacktrace) {
      return _getErrorLog(
        msg: e.toString(),
        stacktrace: stacktrace,
      );
    }
  }

  Result<T> _buildResult<T>(DocumentSnapshot snapshot) {
    if (snapshot.data() == null) {
      return Result.success(
        log: Log.w(
          msg: _buildMessage(Err.recordNotFound),
          stacktrace: StackTrace.current,
        ),
      );
    }

    final data = (snapshot.data() as Map).cast<String, Object?>();
    data[Constants.id] = snapshot.id;

    try {
      final obj = _fromJsonFunc!(data) as T;
      return Result<T>.success(obj: obj);
    } catch (e, st) {
      return _getErrorLog(msg: e.toString(), stacktrace: st);
    }
  }

  @override
  Future<Result<List<T>>> getInById<T>({required List<dynamic> list}) async {
    return await getIn<T>(list: list, fieldName: FieldPath.documentId);
  }

  @override
  Future<Result<List<T>>> getIn<T>(
      {required List<dynamic> list, required dynamic fieldName}) async {
    try {
      assert(ListUtils.instance.isNotEmpty(list));
      assert(fieldName != null);

      QuerySnapshot snapshot =
          await _collectionPath!.where(fieldName, whereIn: list).get();

      return _toList<T>(snapshot);
    } catch (e, stacktrace) {
      return _getErrorLog(
        msg: e.toString(),
        stacktrace: stacktrace,
      );
    }
  }

  Future<Result> deleteById(String id) async {
    try {
      assert(StringUtils.instance.isNotBlank(id));

      Logger.log(
          log: Log.i(
              _buildMessage('Attempting to delete an object with id $id')));
      await _collectionPath!.doc(id).delete();

      return Result.success();
    } catch (e, stacktrace) {
      return _getErrorLog(
        msg: e.toString(),
        stacktrace: stacktrace,
      );
    }
  }

  Result<List<T>> _toList<T>(QuerySnapshot snapshot) {
    if (snapshot.docs.isEmpty) {
      return Result.success(
          log: Log.w(
        msg: _buildMessage('No records have been found'),
      ));
    }

    final List<T?> list = snapshot.docs.map((e) {
      final data = (e.data() as Map).cast<String, Object?>();
      data[Constants.id] = e.id;
      try {
        return _fromJsonFunc!(data) as T;
      } catch (e) {
        Logger.error(StackTrace.current,
            _buildMessage('An error occurred while using func'));
        return null;
      }
    }).toList();

    final withoutNull =
        list.where((element) => element != null).toList() as List<T>? ?? [];

    return Result.success(
      obj: withoutNull,
      // TODO
      // alt: snapshot.docs.last,
    );
  }

  /*
  Get a list of documents
   */

  Result<List<T>> _resultFromQuerySnapshot<T>(QuerySnapshot snapshot) {
    List<T> dtoList = [];

    if (ListUtils.instance.isEmpty(snapshot.docs)) {
      String msg =
          _buildMessage('The received snapshot or its documents is null');
      Logger.log(log: Log.w(msg: msg));
      return Result.success(obj: dtoList, log: Log.w(msg: msg));
    }

    if (snapshot.docs.isEmpty) {
      Result.success(
        obj: [],
        log: Log.w(
          msg: _buildMessage('No records have been found'),
        ),
      );
    }

    for (QueryDocumentSnapshot doc in snapshot.docs) {
      try {
        final data = (doc.data() as Map).cast<String, Object?>();
        data[Constants.id] = doc.id;
        dtoList.add(_fromJsonFunc!(data) as T);
      } catch (e, stacktrace) {
        Logger.log(
          log: Log.e(
            _buildMessage('Record ${doc.id} has an error.\n${e.toString()}'),
            stacktrace: stacktrace,
          ),
        );
      }
    }

    return dtoList.isEmpty
        ? Result.success(
            obj: [],
            log: Log.w(
              msg: _buildMessage('No records have been found'),
            ),
          )
        : Result.success(obj: dtoList, alt: snapshot.docs.last);
  }

  Stream<QuerySnapshot> stream(CollectionReference ref) {
    return ref
        // .limit(limit) // TODO
        .snapshots();
  }

  Future<Result> callFunction({
    required final String functionName,
    required final Map<String, dynamic>? data,
    final int durationSeconds = 30,
  }) async {
    try {
      final HttpsCallable callable = FirebaseFunctions.instance.httpsCallable(
          functionName,
          options: HttpsCallableOptions(
              timeout: Duration(seconds: durationSeconds)));

      final HttpsCallableResult result = await callable.call(data);

      return Result.success(obj: result.data);
    } catch (e, stacktrace) {
      return Result.error(
          log: Log(
        msg: e.toString(),
        stacktrace: stacktrace,
      ));
    }
  }

  @override
  delete(IDs ids) async {
    await _collectionPath!.doc(ids.id).delete();
  }

  @override
  RemoteRepo fromJson(FromJsonFunc fromJsonFunc) {
    _fromJsonFunc = fromJsonFunc;
    return this;
  }

  @override
  Future<Result<List<T>>> getAll<T>({
    int? limit,
    String? orderByField,
    bool descending = false,
    Object? field,
    Map<String, String>? isEqualTo,
    Object? isNotEqualTo,
    Object? isLessThan,
    Object? isLessThanOrEqualTo,
    Object? isGreaterThan,
    Object? isGreaterThanOrEqualTo,
    Object? arrayContains,
    List<Object?>? arrayContainsAny,
    List<Object?>? whereIn,
    List<Object?>? whereNotIn,
    bool? isNull,
    Query? query,
  }) async {
    try {
      Logger.log(log: Log.i(_buildMessage('Attempting to get a list')));

      Query _query = query ?? _collectionPath!;

      if (field != null) {
        _query = _query.where(field,
            whereIn: whereIn,
            arrayContains: arrayContains,
            // isEqualTo: isEqualTo,
            isLessThan: isLessThan,
            isLessThanOrEqualTo: isLessThanOrEqualTo,
            arrayContainsAny: arrayContainsAny,
            isGreaterThan: isGreaterThan,
            isGreaterThanOrEqualTo: isGreaterThanOrEqualTo,
            isNull: isNull);
      }

      if (limit != null) {
        _query = _query.limit(limit);
      }
      if (isEqualTo != null) {
        for (final key in isEqualTo.keys) {
          _query = _query.where(key, isEqualTo: isEqualTo[key]);
        }
      }
      // if (startAfterDoc != null) {
      //   _query = _query.startAfterDocument(startAfterDoc);
      // }

      QuerySnapshot snapshot = await _query.get();
      return _resultFromQuerySnapshot<T>(snapshot);
    } catch (e, stacktrace) {
      return _getErrorLog(
        msg: e.toString(),
        stacktrace: stacktrace,
      );
    }
  }
}
