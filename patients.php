<?php

require_once(__DIR__ . '/../../init.php');
suspendJsonRpc();
require_once(__DIR__ . '/../../map/common/uniDicts.php');
require_once(__DIR__ . '/../../map/common/storage.php');
require_once(__DIR__ . '/../../map/common/addresses.php');
require_once(__DIR__ . '/../../map/common/addressPlaceTypes.php');
require_once(__DIR__ . '/../../map/common/addressStreetTypes.php');
resumeJsonRpc();

class med_common_patients {

    const MAX_PAGE_SIZE = 30;

    private $db; // для пакетного запуска функций, сохраняет коннект

    /**
     * Функция декодирования параметров фильтрации
     * @return string
     */

    private function getFilter($filters) {
        if (isset($filters) && !empty($filters)) {
            $sb = array();
            foreach ($filters as $filter) {
                if (isset($filter->revert)) {
                    $sb[] = "($filter->value $filter->property)";
                } else {
                    $sb[] = "($filter->property $filter->value)";
                }
            }
            return implode(' and ', $sb);
        } else {
            return '(1=1)';
        }
    }

    /**
     * Функция декодирования сортировки
     * @param type $sortstr
     * @return string
     */
    private function getSort($sortstr) {
        if (isset($sortstr)) {
            $sorters = json_decode($sortstr, false);
            $sb = array();
            foreach ($sorters as $sort) {
                $sb[] = "$sort->property $sort->direction";
            }
            return implode(', ', $sb);
        } else {
            return '1';
        }
    }

    /**
     * Функция проверки штрих-кода
     * @param string $params
     * @return bool
     */
    public function checkBarCode($params = null) {
        $db = $this->db ?: $this->db = connectDb();
        $sql = <<<"EOT"
SELECT EXISTS(SELECT BarCode FROM MedBarCode WHERE BarCode LIKE ?) AS "res"
EOT;
        $stmt = $db->prepare($sql);
        $stmt->execute([$params]);
        return $stmt->fetch()['res'];
    }

    /**
     * Возвращает фотографию пациента.
     * @param object $params Параметры.
     * @param integer $params->id MedPatientCode.
     */
    public function readPhoto($params) {
        $cache = getSessionValue('medCommonPatientPhotoCache');
        header('Content-type: image/jpeg');
        header('Cache-Control: max-age=31536000');
        if (isset($cache)) {
            echo base64_decode($cache);
        } else {
            $db = $this->db ?: $this->db = connectDb();
            $sql = <<<"EOT"
select photoId from medPatient
where MedPatientCode = ?
EOT;
            $stmt = $db->prepare($sql);
            $stmt->execute([
                $params->id
            ]);
            $patientGUID = $stmt->fetchAll()[0]['photoid'];
            if ($patientGUID != null) {
                $storage = new map_common_storage();
                $fileName = "patients-photo/" . mb_substr($patientGUID, 0, 1) . "/" . mb_substr($patientGUID, 1, 2) . "/" . $patientGUID . ".jpg";
                $raw = base64_encode($storage->readFile(2, $fileName));
                echo base64_decode($raw);
            }
        }
    }

    /**
     * Загружает фотографию пациента во временное хранилище.
     */
    public function updatePhoto() {
        setSessionValue('medCommonPatientPhotoCache', base64_encode(file_get_contents($_FILES['photo']['tmp_name'])));
        unlink($_FILES['photo']['tmp_name']);
        $rec = new stdClass;
        $rec->success = true;
        echo json_encode($rec);
    }

    /**
     * Загружает фотографию пациента со сканера во временное хранилище.
     */
    public function updatePhotoScan($params) {
        setSessionValue('medCommonPatientPhotoCache', $params->file);
        return true;
    }

    /**
     * Загружает фотографию пациента в постоянное хранилище.
     * @param object $params Параметры.
     * @param integer $params->id MedPatientCode.
     */
    public function commitPhoto($params) {
        $db = $this->db ?: $this->db = connectDb();
        if ($params->action === "ok") {
            if (!is_null(getSessionValue('medCommonPatientPhotoCache'))) {
                $storage = new map_common_storage();
                $image = base64_decode(getSessionValue('medCommonPatientPhotoCache'));
                $sql = <<<"EOT"
select photoId from medPatient
where MedPatientCode = ?
EOT;
                $stmt = $db->prepare($sql);
                $stmt->execute([
                    $params->id
                ]);
                $patientGUID = $stmt->fetchAll()[0]['photoid'];
                if ($patientGUID === null) {
                    $patientGUID = generateUUID();
                    $sql = <<<"EOT"
update MedPatient set
       photoId =  ?
where MedPatientCode = ?
EOT;
                    $stmt = $db->prepare($sql);
                    $stmt->execute([
                        $patientGUID,
                        $params->id
                    ]);
                }
                $fileName = "patients-photo/" . mb_substr($patientGUID, 0, 1) . "/" . mb_substr($patientGUID, 1, 2) . "/" . $patientGUID . ".jpg";
                $storage->writeFile(2, $fileName, $image);
            }
        }
        setSessionValue('medCommonPatientPhotoCache', null);
        return true;
    }

    /**
     * Удаляет фотографию пациента из постоянного хранилища.
     * @param object $params Параметры.
     * @param integer $params->id MedPatientCode.
     */
    public function deletePhoto($params) {
        $db = $this->db ?: $this->db = connectDb();
        $sql = <<<"EOT"
select photoId from medPatient
where MedPatientCode = ?
EOT;
        $stmt = $db->prepare($sql);
        $stmt->execute([
            $params->id
        ]);
        $patientGUID = $stmt->fetchAll()[0]['photoid'];
        if ($patientGUID != null) {
            $storage = new map_common_storage();
            $fileName = "patients-photo/" . mb_substr($patientGUID, 0, 1) . "/" . mb_substr($patientGUID, 1, 2) . "/" . $patientGUID . ".jpg";
            $storage->deleteFile(2, $fileName);
        }
        return true;
    }

    /**
     * Функция получения следующего свободного номера для амбулаторной карты
     * @param string $params - стартовый NumberHistorySickness от которого начинать поиск
     * @return string NumberHistorySickness
     */
    public function getNextNumberHistorySickness($params = null) {
        $db = $this->db ?: $this->db = connectDb();
        $enterpriseCode = getSessionValue('mapEnterpriseCode');
// Если передан начальный параметр для поиска
        if (isset($params) && $params != "") {
            $sql = <<<"EOT"
SELECT
    CASE EXISTS(
            SELECT MedPatientCode
            FROM MedPatient
            WHERE EnterpriseCode = $enterpriseCode
              AND NumberHistorySickness = '$params'
            LIMIT 1)
        WHEN false THEN '$params'
        WHEN true  THEN (
            SELECT INCSTR(MIN(p.NumberHistorySickness))
            FROM MedPatient AS p
            LEFT OUTER JOIN MedPatient AS pn ON pn.NumberHistorySickness = INCSTR(p.NumberHistorySickness) AND pn.EnterpriseCode = p.EnterpriseCode
            WHERE p.EnterpriseCode = $enterpriseCode
                AND p.NumberHistorySickness >= '$params'
                AND pn.MedPatientCode IS NULL)
    END AS "nextNumber"
EOT;
        } else {
            $sql = <<<"EOT"
SELECT INCSTR(COALESCE(asa_trim((
SELECT MAX(REPEAT(' ', 20 - LENGTH(NumberHistorySickness)) + NumberHistorySickness) as Num
FROM MedPatient
WHERE EnterpriseCode = $enterpriseCode)), '0')) AS "nextNumber"
EOT;
        }
        $stmt = $db->prepare($sql);
        $stmt->execute();
        $numberHistorySickness = $stmt->fetch()['nextNumber'];
        return $numberHistorySickness;
    }

    /**
     * Функция получения следующего свободного номера для амбулаторной карты
     * @param string $params - стартовый NumberHistorySickness от которого начинать поиск
     * @return string NumberHistorySickness
     */
    public function checkNextNumberHistorySickness($number = null) {
        $db = $this->db ?: $this->db = connectDb();

        $sql = <<<"EOT"
SELECT
    *
FROM
    MedPatient
WHERE
    numberHistorySickness = '$number'
LIMIT
    1
EOT;

        $stmt = $db->prepare($sql);
        $stmt->execute();
        $PatientCode = $stmt->fetch()['medpatientcode'];

        return $PatientCode;
    }

    /**
     * Функция определения поликлиники, учаска и спецучастков пациента
     * @param $patient {Object} пациент
     * @param $busy {Object} актуальная занятость
     * @return {object/false} объект результата или false, если ничего не определено
     *  clinicId (int) код поликлиники
     *  sectorId (int) код участка
     *  addSectors [Array] массив id спецучастков
     */
    public function detectPatientGroup($patient, $busy = null) {

        function createRange() {
            $result = new stdClass();
            $result->_all = null;  // использовать весь диапазон
            $result->_from = null; // начиная с
            $result->_to = null;   // оканчивая
            $result->_even = null; // -1 - как у предка, 0-без разницы, 1 - нечет, 2 - чет
            $result->range = []; // houses / corpus / flats
            return $result;
        }

        function getLeft($_str) {
            $words = preg_split("/[\s-]+/u", $_str);
            return trim($words[0]);
        }

        function getRight($_str) {
            $words = preg_split("/[\s-]+/u", $_str);
            $result = $words[0];
            if (count($words) > 1) {
                $result = $words[1];
            }
            return trim($result);
        }

        function getEven(&$ix, $_str) {
            if (($ix < strlen($_str)) && ($_str[$ix] == '%')) {
                $_val = '';
                $ix += 1;
                while ($ix < strlen($_str)) {
                    if (($_str[$ix] >= '0') && ($_str[$ix] <= '9')) {
                        $_val = $_val . $_str[$ix];
                    } else {
                        break;
                    }
                    $ix += 1;
                }
                $result = (int) $_val;
            } else {
                $result = 0;
            }
            return $result;
        }

        function getRange(&$ix, $_str) {
            $result = null;
            // выберем блок
            $_range = '';
            $_brack = false;
            while ($ix < strlen($_str)) {
                $ch = $_str[$ix];
                if ($ch == '[') {
                    if ((!$_brack) && (strlen($_range) !== 0)) {
                        break;
                    }
                    $_brack = true;
                } else {
                    if (($ch == ']') || ($ch == '/') || ($ch == ' ') || ($ch == '%')) {
                        if (($ch == ']') || ($ch == ' ')) {
                            $ix += 1;
                        }
                        break;
                    } else {
                        $_range = $_range . $ch;
                    }
                }
                $ix += 1;
            }
            // разберем блок
            $parts = preg_split("/[,]+/u", $_range);
            if (!count($parts) && !$_brack) {
                return $result;
            }
            $result = createRange();
            $result->_all = strlen($_range) == 0;
            $result->_even = getEven($ix, $_str);
            if (!$result->_all) { // внутри есть перечисление
                foreach ($parts as $part) {
                    $new_range = createRange();
                    $new_range->_all = false;
                    $new_range->_from = getLeft(trim($part));
                    $new_range->_to = getRight(trim($part));
                    $new_range->_even = -1;
                    array_push($result->range, $new_range);
                }
            }
            return $result;
        }

        function isNext(&$ix, $_str, $_what) {
            $result = false;
            $fIx = $ix;
            while ($fIx < strlen($_str)) {
                if ($_str[$fIx] == $_what) {
                    $ix = $fIx + 1;
                    $result = true;
                    break;
                } else {
                    if ($_str[$fIx] !== ' ') {
                        break;
                    }
                }
                $fIx += 1;
            }
            return $result;
        }

        function getAddrRange($house) {
            $house = str_replace(array('\r\n', '\r', '\n', '\t'), ';', $house);
            $house = trim($house);
            // Нормализация настройки -->
            $house = preg_replace('/\s+/', ' ', $house);
            $house = preg_replace('/,\s+/', ',', $house);
            $house = preg_replace('/\s+,/', ',', $house);
            $house = preg_replace('/\[\s+/', '[', $house);
            $house = preg_replace('/\s+\[/', '[', $house);
            $house = preg_replace('/\s+\]/', ']', $house);
            $house = preg_replace('/\]\s+/', ']', $house);
            $house = preg_replace('/-\s+/', '-', $house);
            $house = preg_replace('/\s+-/', '-', $house);
            $house = preg_replace('/%\s+/', '%', $house);
            $house = preg_replace('/\s+%/', '%', $house);
            $house = preg_replace('/\/\s+/', '/', $house);
            $house = preg_replace('/\s+\//', '/', $house);
            $house = preg_replace('/;\s+/', ';', $house);
            $house = preg_replace('/\s+;/', ';', $house);
            // <--
            $parts = preg_split("/[;]+/u", $house);
            $housesInfo = [];
            foreach ($parts as $part) {
                $housesInfo[] = $rec = new stdClass;
                $ix = 0;
                $rec->houses = getRange($ix, trim($part));
                if (isNext($ix, $part, '/')) {
                    $rec->corpuses = getRange($ix, trim($part));
                } else {
                    $rec->corpuses = null;
                }
                $rec->flats = getRange($ix, trim($part));
            }
            return $housesInfo;
        }

        function testEven($val, $even) {
            $result = false;
            if (!($even == 2) || ($even = 1)) {
                $result = true;
            } else {
                $result = ((((int) $val % 2) == 1) && ($even == 1)) || ((((int) $val % 2) == 0) && ($even == 2));
            }
            return $result;
        }

        function isNeedToBeEmpty($val) {
            return ($val == '~');
        }

        function testRange($val, $range) {
            $result = false;
            if ((isNeedToBeEmpty($range->_from) || isNeedToBeEmpty($range->_to) ) && !($val)) {
                $result = true;
                return $result;
            }
            if (($range->_from == $range->_to) && ($val == $range->_from) && !isNeedToBeEmpty($range->_from)) {
                $result = true;
            } else {
                $result = (($val >= $range->_from) && ($val <= $range->_to));
            }
            return $result;
        }

        function testValue($val, $settings) {
            $result = false;
            if (!$settings) {
                $result = true;
                return $result;
            }
            $result = testEven($val, $settings->_even);
            if ($result && !$settings->_all) {
                $result = false;
                foreach ($settings->range as $range) {
                    if (testRange($val, $range)) {
                        $result = true;
                        return $result;
                    }
                }
            }
            return $result;
        }

        $db = $this->db ?: $this->db = connectDb();
        // Получаем адрес жительства или прописки (жительства в приоритете) -->
        $addressResideId = isEmptyProperty($patient, 'addressResideId', null);
        $addressId = $addressResideId ? $addressResideId : isEmptyProperty($patient, 'addressId', null);
        $addressIds = array_unique([$addressId, isEmptyProperty($patient, 'addressId', null)]);
        $result = new stdClass();
        $result->clinicId = null;
        $result->sectorId = null;
        $result->addSectors = [];
        $flMasterFound = false;
        for ($index = 0; $index < count($addressIds); $index++) {
            $addressId = $addressIds[$index];
            if ($addressId == null) {
                continue;
            }
            $prefix = ($addressId == $addressResideId) ? 'Reside' : '';
            $house = isEmptyProperty($patient, 'address' . $prefix . 'House', null);
            $corps = isEmptyProperty($patient, 'address' . $prefix . 'Corps', null);
            $flat = isEmptyProperty($patient, 'address' . $prefix . 'Flat', null);
            if (!$addressId) {
                return $result;
            }
            $address = new map_common_addresses();
            $tmpParams = new stdClass();
            $tmpParams->id = $addressId;
            $address = $address->readAddresses($tmpParams);
            $address = (object) $address->items[0];
            if (!$address) {
                return $result;
            }
            // <--
            $dateOfBirth = Date("Ymd", strtotime(isEmptyProperty($patient, 'dateOfBirth')));
            $sqlAge = "select (EXTRACT(YEAR FROM age(today(), $dateOfBirth))) as age";
            $stmtAge = $db->prepare($sqlAge);
            $stmtAge->execute();
            // Параметры фильтра настроек -->
            $age = $stmtAge->fetch()['age'];
            $sex = isEmptyProperty($patient, 'sex', 0);
            $country = isEmptyProperty($address, 'country', '');
            $area = isEmptyProperty($address, 'area', '');
            $areaType = isEmptyProperty($address, 'areaType', '');
            $district = isEmptyProperty($address, 'district', '');
            $region = isEmptyProperty($address, 'region', '');
            $place = isEmptyProperty($address, 'place', '');
            $placeType = isEmptyProperty($address, 'placeType', '');
            $ward = isEmptyProperty($address, 'ward', '');
            $wardType = isEmptyProperty($address, 'wardType', '');
            $street = isEmptyProperty($address, 'street', '');
            $streetType = isEmptyProperty($address, 'streetType', '');
            // <--
            // Выборка настроек -->
            $sql = <<<"EOT"
SELECT mpl.medPatientLocationCode   "id",
       mpl.medPatientGroupCode      "sectorId",
       mpl.special                  "special",
       mpl.medPatientPolyclinicCode "clinicId",
       mpl.searchType               "searchType",
       mpl.useStartAge              "useStartAge",
       mpl.startAge                 "startAge",
       mpl.useFinishAge             "useFinishAge",
       mpl.finishAge                "finishAge",
       mpl.sex                      "sex",
       mpla.country                 "country",
       mpla.area                    "area",
       mpla.areaType                "areaType",
       mpla.district                "district",
       mpla.region                  "region",
       mpla.place                   "place",
       mpla.placeType               "placeType",
       mpla.ward                    "ward",
       mpla.wardType                "wardType",
       mpla.street                  "street",
       mpla.streetType              "streetType",
       mpla.house                   "house",
       mplb.enterpriseKindCode      "organizationKindId",
       mplb.enterpriseCode          "organizationId",
       mplb.departmentCode          "departmentId"
FROM medPatientLocation mpl
LEFT JOIN medPatientLocationAddress mpla ON mpla.medPatientLocationCode = mpl.medPatientLocationCode
      /* Настройка типа поиска AND COALESCE(mpl.searchType, 0) = 0 */
      /*Фильтр по адресам*/
      AND (COALESCE(mpla.country, '') = '' OR '$country' LIKE mpla.country || '%')
      AND (COALESCE(mpla.area, '') = '' OR '$area' LIKE mpla.area || '%')
      AND (COALESCE(mpla.areaType, '') = '' OR '$areaType' LIKE mpla.areaType || '%')
      AND (COALESCE(mpla.district, '') = '' OR '$district' LIKE mpla.district || '%')
      AND (COALESCE(mpla.region, '') = '' OR '$region' LIKE mpla.region || '%')
      AND (COALESCE(mpla.place, '') = '' OR '$place' LIKE mpla.place || '%')
      AND (COALESCE(mpla.placeType, '') = '' OR '$placeType' LIKE mpla.placeType || '%')
      AND (COALESCE(mpla.ward, '') = '' OR '$ward' LIKE mpla.ward || '%')
      AND (COALESCE(mpla.wardType, '') = '' OR '$wardType' LIKE mpla.wardType || '%')
      AND (COALESCE(mpla.street, '') = '' OR '$street' LIKE mpla.street || '%')
      AND (COALESCE(mpla.streetType, '') = '' OR '$streetType' LIKE mpla.streetType || '%')
LEFT JOIN medPatientLocationBusy mplb ON mplb.medPatientLocationCode = mpl.medPatientLocationCode AND mpl.searchType = 1
WHERE (mpla.medpatientlocationaddressCode IS NOT NULL OR mplb.medpatientlocationbusyCode IS NOT NULL)
  AND (COALESCE(mpl.sex, 2) = 2 OR COALESCE(mpl.sex, 0) = $sex)              /*Фильтр по настройке пола*/
  AND (COALESCE(mpl.useStartAge, 0) = 0 OR COALESCE(mpl.startAge) <= $age)   /*Возраст от*/
  AND (COALESCE(mpl.useFinishAge, 0) = 0 OR COALESCE(mpl.finishAge) >= $age) /*Возраст до*/
EOT;
            $stmt = $db->prepare($sql);
            $stmt->execute();
            $settings = $stmt->fetchAll();
            // <--
            // DEBUG -->
            // getAddrRange ('   [  1   -  100  ,   200-300]  /  2   3 ;  10  %  2/1;12/1;12/1;14/1;8a;26/1;26/2;18/1;18/3;18/4;28');
            // <--
            // Если найдены подходящие настройки, продолжаем поиск
            if ($settings) {
                foreach ($settings as $setting) {
                    $setting = (object) $setting;
                    // Если найден главный участок и поликлиника, то пропускаем запись
                    if ($flMasterFound && !$setting->special) {
                        continue;
                    }
                    // Получаем массив настроек по домам
                    if ($setting->house) {
                        $houseSettings = getAddrRange($setting->house);
                        foreach ($houseSettings as $houseSetting) {
                            // TODO Test house setting
                            if (testValue($house, $houseSetting->houses) &&
                                    testValue($corps, $houseSetting->corpuses) &&
                                    testValue($flat, $houseSetting->flats)) {
                                // Настройка подошла
                                // Если спецучасток
                                if ($setting->special) {
                                    array_push($result->addSectors, $setting->sectorId);
                                    if ($setting->clinicId && !isEmptyProperty($result, 'clinicId', false)) {
                                        $result->clinicId = $setting->clinicId;
                                    }
                                } else {
                                    // Определяем главыный участок только на первой итерации
                                    if ($index == 0) {
                                        $flMasterFound = true;
                                        $result->clinicId = $setting->clinicId;
                                        $result->sectorId = $setting->sectorId;
                                    }
                                }
                            }
                        }
                    } else {
                        if ($setting->special) {
                            array_push($result->addSectors, $setting->sectorId);
                        } else {
                            // Определяем главыный участок только на первой итерации
                            if ($index == 0) {
                                $flMasterFound = true;
                                $result->clinicId = $setting->clinicId;
                                $result->sectorId = $setting->sectorId;
                            }
                        }
                    }
                }
            }
        }
        if ($flMasterFound || count($result->addSectors)) {
            return $result;
        } else {
            return false;
        }
    }

    /**
     * Выбирает список пациентов
     * @param {object} $params
     *  search (string) строка поиска
     *  ---- автоматически добавляемые параметры -----
     *  filter (string) поля по которому производится фильтрация
     *  sort (string) поля по которому производится сортировка
     *  page (int) текущий номер страницы
     *  limit (int) количество записей на странице
     * @return {object}
     *  id (int) id врача
     *  manId (int) id человека
     *  fio (string) фио
     */
    public function readPatients($params) {
        $db = $this->db ?: $this->db = connectDb();
        $enterpriseCode = getSessionValue('mapEnterpriseCode');
        $patientId = isEmptyProperty($params, 'patientId', 0);
        $storage = new map_common_storage();
        $currDate = Date("Ymd");
        $allCondSearch = "";
        $cipher = "";
        $aFio = "";
        $filters = array();
        $filterstr = isEmptyProperty($params, 'filter');
        if (isset($filterstr)) {
            $index = "";
            $filters = json_decode($filterstr, false);
            foreach ($filters as $search) {
                if ($search->property === 'search') {
                    //$aFio = trim($search->value);
                    $search = explode('|', $search->value);
                    $index = array_search($search, $filters);
                }
            }
            unset($filters[$index]);
        }

        foreach ($search as $aFio) {
            $aFio = trim($aFio);

// поиск (по ФИО и штрихкоду)
            if ($aFio === "") {
                continue;
            }

            $condSearch = "";
            unset($adressType);
            unset($adressPlace);
            unset($adressResideType);
            unset($adressResidePlace);

            $aFio = mb_strtolower($aFio, 'UTF-8');
            $matches = array();
            $reg = '/^(\+|\-|\ |\(|\)|\d)*$/';
            $count = "";
            if (preg_match_all($reg, $aFio, $matches, PREG_SET_ORDER, 0)) { // Проверка, состоит ли строка только из чисел, +, -, пробелов, скобок
                $numbString = preg_replace("/[^0-9]/", '', $matches[0][0]); // Удаление из строки всего, кроме цифр
                $count = iconv_strlen($numbString); // кол-во цифр в строке
                $rez = str_split($numbString); // массив из цифр строки
            }
// формируем запрос на поиск по числовым полям
            if ($count) {
                $value = implode("%", $rez); //склейка цифр для запроса в базу
                if ($count >= 16) {
                    $condSearch .= "EXISTS(select 1
                                          from MedBarCode
                                          where BarCode LIKE '$numbString'
                                            and ManCode = mp.ManCode
                                          limit 1)";
                    $condSearch .= " or mp.NumberHistorySickness LIKE '%$numbString%'";
                } else {
                    $condSearch .= " mp.NumberHistorySickness LIKE '%$numbString%'";
                } if ($count >= 5) {
                    $condSearch .= " or EXISTS(select PhoneNumber
                                          from ManPhone
                                          where PhoneNumber LIKE '%$value'
                                            and ManCode = mp.ManCode)";
                }
            } else {
// парсим ФИО (последний пустой не обрабатывается, только первые три если заполнены)
                $fio = preg_split("/[\s.,]+/u", $aFio);
// собираем условия поиска
                $condSearch = "M.SurName LIKE '$fio[0]%'";
                if (isset($fio[1]) && $fio[1] !== "") {
                    $condSearch .= " and M.Name LIKE '$fio[1]%'";
                }
                if (isset($fio[2]) && $fio[2] !== "") {
// собираем все что осталось в отчество
                    $fio[2] = trim(implode(" ", (array_slice($fio, 2))));
                    $condSearch .= " and M.SecondName LIKE '$fio[2]%'";
                }
// парсим строку в адрес
                $condSearch = '(' . $condSearch . ')';
                preg_match_all('~"([^"]*)"~u', $aFio, $street);
                if (count($street[0]) != 0) {
                    $aFio = str_replace($street[0][0], "", $aFio);
                    $adress = preg_split('/[\s\"]+/u', $aFio); //"/[\s.\/]+/"
                    array_splice($adress, 1, 0, $street[1]);
                } else {
                    $adress = preg_split('/[\s\"]+/u', $aFio); //"/[\s.\/]+/"
                }
                $street = preg_split("/[.]+/", $adress[0]);
                if (isset($street[1]) && $street[1] === "" && isset($adress[1])) {
                    $street[1] = $adress[1];
                    $adress[0] = $adress[0] . $adress[1];
                    unset($adress[1]);
                }
// ищем префикс типа адреса в БД и формируем часть запроса
                if (isset($street[1])) {
                    if (!isset($addressPlaceTypes)) {
                        $addressPlaceTypes = new map_common_addressPlaceTypes();
                        $addressStreetTypes = new map_common_addressStreetTypes();
                        $streetType = $addressStreetTypes->readAddressStreetTypes();
                        $placeType = $addressPlaceTypes->readAddressPlaceTypes();
                    }
                    switch (mb_strtolower($street[0], 'UTF-8')) {
                        case "у":
                            $street[0] = 'ул';
                            break;
                        case "пер":
                            $street[0] = 'п';
                            break;
                        case "дер":
                            $street[0] = 'д';
                            break;
                    }
                    foreach ($streetType as $value) {
                        if (!is_numeric($value['shortName']) && $value['shortName']) {
                            if ($value['shortName'][strlen($value['shortName']) - 1] === '.') {
                                $value['shortName'] = substr($value['shortName'], 0, -1);
                            }
                            if ($street[0] === mb_strtolower($value['shortName'], 'UTF-8')) {
                                $adressType = " and ar.AddressStreetTypeCode=" . $value['id'] . "";
                                $adressPlace = "ar.street LIKE '%$street[1]%'";
                                $adressResideType = " and a.AddressPlaceTypeCode = " . $value['id'] . "";
                                $adressResidePlace = "a.place LIKE '%$street[1]%'";
                                break;
                            }
                        }
                    }
                    foreach ($placeType as $value) {
                        if (!is_numeric($value['shortName']) && $value['shortName']) {
                            if ($value['shortName'][strlen($value['shortName']) - 1] === '.') {
                                $value['shortName'] = substr($value['shortName'], 0, -1);
                            }
                            if ($street[0] === mb_strtolower($value['shortName'], 'UTF-8')) {
                                if (isset($street[2])) {
                                    $adressPlace = "ar.place LIKE '%$street[1].$street[2]%'";
                                    $adressResidePlace = "a.place LIKE '%$street[1].$street[2]%'";
                                    $adressResideStreet = " or (a.place LIKE '%$street[1].$street[2]%'";
                                    $adressStreet = " or (ar.place LIKE '%$street[1].$street[2]%'";
                                } else {
                                    $adressPlace = "ar.place LIKE '%$street[1]%'";
                                    $adressResidePlace = "a.place LIKE '%$street[1]%'";
                                    $adressResideStreet = " or (a.place LIKE '%$street[1]%'";
                                    $adressStreet = " or (ar.place LIKE '%$street[1]%'";
                                }
                                $adressType = " and ar.AddressPlaceTypeCode = " . $value['id'] . "";
                                $adressResideType = " and a.AddressPlaceTypeCode = " . $value['id'] . "";
                                $adressResideTypeStreet = $adressResideType;
                                $adressTypeStreet = $adressType;
                                break;
                            }
                        }
                    }
                }
                if (!isset($adressType)) {
                    $adressResideStreet = " or (a.place LIKE '%$adress[0]%'";
                    $adressResideTypeStreet = "";
                    $adressTypeStreet = "";
                    $adressStreet = " or (ar.place LIKE '%$adress[0]%'";
                }
// формируем запрос на поиск по адресу
                if (isset($adress[1]) && $adress[1] !== "") {
                    $localAdress = preg_split("/[\\\s\/]+/", $adress[1]); //"/[\s.\/]+/"
                    if (!isset($adressType)) {
                        $adressType = "";
                        $adressPlace = "((ar.street LIKE '%$street[0]%') or (ar.place LIKE '%$street[0]%'))";
                        $adressResideType = "";
                        $adressResidePlace = "((a.street LIKE '%$street[0]%') or (a.place LIKE '%$street[0]%'))";
                    }
                    $adressSearch = "$adressResidePlace $adressResideType";
                    $adressSearch .= " and pass.AddressResideHouse LIKE '$localAdress[0]'";
                    if (isset($localAdress[1]) && $localAdress[1] !== "") {
                        $adressSearch .= " and pass.AddressResideCorps LIKE '$localAdress[1]'";
                    }
                    if (isset($adress[2]) && $adress[2] !== "") {
                        $adressSearch .= " and pass.AddressResideFlat LIKE '$adress[2]'";
                    }
                    $adressSearch = '(' . $adressSearch . ') or (';
                    $adressSearch .= "$adressPlace $adressType";
                    $adressSearch .= " and pass.AddressHouse LIKE '$localAdress[0]'";
                    if (isset($localAdress[1]) && $localAdress[1] !== "") {
                        $adressSearch .= " and pass.AddressCorps LIKE '$localAdress[1]'";
                    }
                    if (isset($adress[2]) && $adress[2] !== "") {
                        $adressSearch .= " and pass.AddressFlat LIKE '$adress[2]'";
                    }
                    $adressSearch = '' . $adressSearch . ')';
                    $adressType = "";
                    $adressPlace = " and ar.street LIKE '%$adress[1]%'";
                    $adressResideType = "";
                    $adressResidePlace = " and a.street LIKE '%$adress[1]%'";
                    $street = preg_split("/[.]+/", $adress[1]);
                    if (isset($street[1])) {
                        $addressPlaceTypes = new map_common_addressPlaceTypes();
                        $addressStreetTypes = new map_common_addressStreetTypes();
                        $streetType = $addressStreetTypes->readAddressStreetTypes();
                        $placeType = $addressPlaceTypes->readAddressPlaceTypes();
                        switch (mb_strtolower($street[0], 'UTF-8')) {
                            case "у":
                                $street[0] = 'ул';
                                break;
                            case "пер":
                                $street[0] = 'п';
                                break;
                            case "дер":
                                $street[0] = 'д';
                                break;
                        }
                        foreach ($streetType as $value) {
                            if (!is_numeric($value['shortName']) && $value['shortName']) {
                                if ($value['shortName'][strlen($value['shortName']) - 1] === '.') {
                                    $value['shortName'] = substr($value['shortName'], 0, -1);
                                }
                                if ($street[0] === mb_strtolower($value['shortName'], 'UTF-8')) {
                                    $adressType = " and ar.AddressStreetTypeCode=" . $value['id'] . "";
                                    $adressResideType = " and a.AddressStreetTypeCode = " . $value['id'] . "";
                                    $adressPlace = " and ar.street LIKE '%$street[1]%'";
                                    $adressResidePlace = " and a.street LIKE '%$street[1]%'";
                                    break;
                                }
                            }
                        }
                    }
                    if (isset($adressResideStreet) && isset($adressResideTypeStreet)) {
                        $adressSearch .= "$adressResideStreet $adressResideTypeStreet";
                    }
                    if (isset($adressResidePlace) && isset($adressResideType)) {
                        $adressSearch .= "$adressResidePlace $adressResideType";
                    }
                    if (isset($adress[2]) && $adress[2] !== "") {
                        $localAdress = preg_split("/[\s\/]|[\s\\\\]+/", $adress[2]);
                    } else {
                        $localAdress = null;
                    }
                    if (isset($localAdress[0]) && $localAdress[0] !== "") {
                        $adressSearch .= " and pass.AddressResideHouse LIKE '$localAdress[0]'";
                    }
                    if (isset($localAdress[1]) && $localAdress[1] !== "") {
                        $adressSearch .= " and pass.AddressResideCorps LIKE '$localAdress[1]'";
                    }
                    if (isset($adress[3]) && $adress[3] !== "") {
                        $adressSearch .= " and pass.AddressResideFlat LIKE '$adress[3]')";
                    } else {
                        $adressSearch .= ")";
                    }
                    $adressSearch .= "$adressStreet $adressTypeStreet";
                    $adressSearch .= "$adressPlace $adressType";
                    if (isset($localAdress[0]) && $localAdress[0] !== "") {
                        $adressSearch .= " and pass.AddressHouse LIKE '$localAdress[0]'";
                    }
                    if (isset($localAdress[1]) && $localAdress[1] !== "") {
                        $adressSearch .= " and pass.AddressCorps LIKE '$localAdress[1]'";
                    }
                    if (isset($adress[3]) && $adress[3] !== "") {
                        $adressSearch .= " and pass.AddressFlat LIKE '$adress[3]')";
                    } else {
                        $adressSearch .= ")";
                    }
                } else {
                    if (isset($adressType)) {
                        $adressSearch = "($adressPlace $adressType) or ($adressResidePlace $adressResideType)";
                    } else {
                        if (isset($street[1])) {
                            $adressSearch = "(((a.street LIKE '%$street[1]%') or (a.place LIKE '%$street[1]%')) or ((ar.street LIKE '%$street[1]%') or (ar.place LIKE '%$street[1]%')))";
                        } else {
                            $adressSearch = "(((a.street LIKE '%$street[0]%') or (a.place LIKE '%$street[0]%')) or ((ar.street LIKE '%$street[0]%') or (ar.place LIKE '%$street[0]%')))";
                        }
                    }
// поиск по личному номеру человека
                    if (preg_match('/\d/', $adress[0]) && (iconv_strlen($adress[0]) == 14)) {
                        $condSearch .= " or (pass.PersonalNumber LIKE '$adress[0]')";
                        $condSearch = '(' . $condSearch . ')';
                    }
                    if (preg_match('/\d/', $adress[0])) {
                        $condSearch .= "or (mp.NumberHistorySickness LIKE '%$adress[0]%')";
                    }
                }
                if (isset($adressSearch)) {
                    $adressSearch = ' or (' . $adressSearch . ') ';
                    $condSearch .= $adressSearch;
                }
            }
            //$condSearch = ' and (' . $condSearch . ') ';
            if ($allCondSearch) {
                $allCondSearch .= ' or ';
            }
            $allCondSearch .= '(' . $condSearch . ')';
        }//foreach($search...

        if ($allCondSearch) {
            $allCondSearch = ' and (' . $allCondSearch . ') ';
        }

        // for PAGE
        $filter = $this->getFilter($filters);
        $sort = $this->getSort(isEmptyProperty($params, 'sort'));

        $page = isEmptyProperty($params, 'page', 1);

        $limit = isEmptyProperty($params, 'limit', self::MAX_PAGE_SIZE);
        $limit = $limit >= self::MAX_PAGE_SIZE ? self::MAX_PAGE_SIZE : $limit;

        $start = (int) (($page - 1) * $limit);
        $start = $start < 0 ? 0 : $start;

        // Определяем сложные поля запроса
        $fieldIsAsctive = <<<"EOT"
(CASE
    WHEN COALESCE(mp.FinishDate, 30000101)::date = 30000101 THEN 1
    ELSE 0
END)
EOT;
        $fieldAge = <<<"EOT"
(EXTRACT(YEAR FROM age($currDate, pass.DateOfBirth)))
EOT;

        // Заменяем поля в фильтре
        $inFilterFields = array(
            "\"isActive\"",
            "\"age1\"",
            "\"age2\"",
            "\"sectorId\"",
            "\"sectorAddId\"",
            "\"contingentId\"");
        $outFilterFields = array(
            $fieldIsAsctive,
            $fieldAge,
            $fieldAge,
            "mp.MedPatientGroupCode",
            "mpag.MedPatientGroupCode",
            "mpc.MedPrivelegeCode");
        $filter = str_replace($inFilterFields, $outFilterFields, $filter);

// основной запрос
        $sqlFromWhere = <<<"EOT"
FROM MedPatient AS mp
JOIN Man AS m ON m.ManCode = mp.ManCode
LEFT JOIN ManPassport AS pass ON pass.ManCode = mp.ManCode
LEFT JOIN MedPatientGroup AS mpg ON mpg.MedPatientGroupCode = mp.MedPatientGroupCode
LEFT JOIN MedPatientAddGroup AS mpag ON mpag.MedPatientCode = mp.MedPatientCode
LEFT JOIN MedPatientGroup AS mpgadd ON mpgadd.MedPatientGroupCode = mpag.MedPatientGroupCode
LEFT JOIN MedPatientInfo AS mpi ON mpi.MedPatientCode = mp.MedPatientCode
/* LEFT JOIN UniList AS ulwp ON ulwp.UniListCode = mpi.UniListWorkPlaceCode */
LEFT JOIN Address AS a ON a.AddressCode = pass.AddressResideCode
LEFT JOIN Address AS ar ON ar.AddressCode = pass.AddressCode
LEFT JOIN AddressPlaceType AS pt ON pt.AddressPlaceTypeCode = a.AddressPlaceTypeCode
LEFT JOIN AddressStreetType AS st ON st.AddressStreetTypeCode = a.AddressStreetTypeCode
/* Подтормаживает, отдельные подзапросы справляются быстрее
LEFT JOIN LATERAL (SELECT ManPhoneCode, PhoneNumber, Note
                   FROM ManPhone
                   WHERE ManCode = mp.ManCode
                     AND PhoneType = 1
                   ORDER BY ManPhoneCode DESC LIMIT 1) phone on TRUE
LEFT JOIN LATERAL (SELECT ManPhoneCode, PhoneNumber, Note
                   FROM ManPhone
                   WHERE ManCode = mp.ManCode
                     AND PhoneType = 3
                   ORDER BY ManPhoneCode DESC LIMIT 1) phoneMobile on TRUE
LEFT JOIN LATERAL (SELECT ManPhoneCode, PhoneNumber, Note
                   FROM ManPhone
                   WHERE ManCode = mp.ManCode
                     AND PhoneType = 4
                   ORDER BY ManPhoneCode DESC LIMIT 1) mail on TRUE
*/
LEFT JOIN MedPatientCard AS mpc ON mpc.MedPatientCode = mp.MedPatientCode AND
                                   $currDate BETWEEN mpc.StartDate AND COALESCE(mpc.FinishDate, 30000101)::date
WHERE mp.EnterpriseCode = $enterpriseCode
  AND mp.FlagGroup IS NULL
  AND (mp.MedPatientCode = $patientId or 0=$patientId)
  AND ($filter)
  $allCondSearch
GROUP BY mp.MedPatientCode, mpg.MedPatientGroupCode, pass.ManPassportCode /*, ulwp.UniListCode, mpc.MedPatientCardCode*/
EOT;

// -- READ COUNT
        $sqlTotal = <<<"EOT"
SELECT
       COUNT(*) AS "total"
FROM (SELECT DISTINCT mp.MedPatientCode
$sqlFromWhere) AS t
EOT;
        $stmtTotal = $db->prepare($sqlTotal);
        $stmtTotal->execute();
        $total = (int) $stmtTotal->fetch()['total'];
        $start = $total < $start ? 0 : $start;

// -- READ DATA
        $sql = <<<"EOT"
SELECT  t.*,
        FN_cmCalcAddress(t."manId", 0)::text as "address",
        FN_cmCalcAddress(t."manId", 1)::text as "addressReside",
        (SELECT string_agg(pg.Name::text, '; ' ORDER BY pg.Name)
         FROM MedPatientAddGroup AS MPAD
         JOIN MedPatientGroup AS pg ON pg.MedPatientGroupCode = MPAD.MedPatientGroupCode
         WHERE MPAD.MedPatientCode = t.id
           AND COALESCE(pg.ShowWithPatient, 0)::integer = 1)::text as "patientGroupAdd",
        (SELECT string_agg(eb.Name::text, '; ' ORDER BY eb.Name)
         FROM MedPatientBusy mb
         LEFT JOIN Enterprise as eb ON mb.EnterpriseCode = eb.EnterpriseCode
         WHERE mb.MedPatientCode = t.id
           AND $currDate BETWEEN mb.StartDate AND mb.FinishDate) as "workPlace",
        FN_medPatientPictureList(t.id, NULL, 2, 1) as "shortStatesList",
        FN_medPatientPictureList(t.id, NULL) as "fullStatesList"
FROM (
    SELECT
        mp.MedPatientCode as "id",
        mp.ManCode as "manId",
        FN_cmCalcAddress(mp.ManCode, 5)::text as "addressDisplay",
        FN_cmFIO(mp.ManCode)::text as "fio",
        mp.Cipher as "cipher",
        mp.PhotoId as "photoId",
        0 as "photoExist",
        mp.NumberHistorySickness as "numberHistorySickness",
        mp.MedPatientGroupCode as "sectorId",
        mp.archivalNumber as "archivalNumber",
        mp.MedPatientPolyclinicCode as "clinicId",
        mp.OutReason as "outReason",
        mpg.Name as "sector",
        mpg.MedDoctorCode as "sectorDoctorId",
        string_agg(distinct(mpgadd.Name::text), '; ') as "sectorAdd",
        string_agg(distinct(mpgadd.MedPatientGroupCode::text), ',') as "sectorAddIds",
        CASE
            WHEN mp.StartDate = 19000101 THEN null
            ELSE mp.StartDate
        END::date "startDate",
        CASE
            WHEN mp.FinishDate = 30000101 THEN null
            ELSE mp.FinishDate
        END::date as "finishDate",
        pass.ManPassportCode as "manPassportId",
        pass.AddressCode as "addressId",
        pass.AddressHouse as "addressHouse",
        pass.AddressCorps as "addressCorps",
        pass.AddressFlat as "addressFlat",
        pass.Zipcode as "addressZipcode",
        pass.AddressResideCode as "addressResideId",
        pass.AddressResideHouse as "addressResideHouse",
        pass.AddressResideCorps as "addressResideCorps",
        pass.AddressResideFlat as "addressResideFlat",
        pass.ZipcodeReside as "addressResideZipcode",
        pass.Sex as "sex",
        pass.DateOfBirth as "dateOfBirth",
        pass.PassportNumber as "passportNumber",
        pass.Authority as "authority",
        pass.DateOfIssue as "dateOfIssue",
        pass.DateOfExpiry as "dateOfExpiry",
        pass.PlaceOfBirth as "placeOfBirth",
        pass.PersonalNumber as "personalNumber",
        random() "random",
        (select string_agg((concat( E',' ,case
            when mpn.phoneType=1 then 'д'
            when mpn.phoneType=2 then 'р'
            when mpn.phoneType=3 then 'м'
            when mpn.phoneType=4 then 'п'
        else null
            end, E'.' ,mpn.phoneNumber))::text, E'' ) from manphone AS mpn where mpn.ManCode=mp.ManCode) as contact,
        (SELECT ManPhoneCode FROM ManPhone WHERE ManCode = mp.ManCode AND PhoneType = 1 ORDER BY ManPhoneCode DESC LIMIT 1) as "phoneId",
        (SELECT PhoneNumber FROM ManPhone WHERE ManCode = mp.ManCode AND PhoneType = 1 ORDER BY ManPhoneCode DESC LIMIT 1) as "phone",
        (SELECT ManPhoneCode FROM ManPhone WHERE ManCode = mp.ManCode AND PhoneType = 3 ORDER BY ManPhoneCode DESC LIMIT 1) as "mobilePhoneId",
        (SELECT PhoneNumber FROM ManPhone WHERE ManCode = mp.ManCode AND PhoneType = 3 ORDER BY ManPhoneCode DESC LIMIT 1) as "mobilePhone",
        (SELECT Note FROM ManPhone WHERE ManCode = mp.ManCode AND PhoneType = 3 ORDER BY ManPhoneCode DESC LIMIT 1) as "mobilePhoneOperator",
        (SELECT ManPhoneCode FROM ManPhone WHERE ManCode = mp.ManCode AND PhoneType = 4 ORDER BY ManPhoneCode DESC LIMIT 1) as "mailId",
        (SELECT PhoneNumber FROM ManPhone WHERE ManCode = mp.ManCode AND PhoneType = 4 ORDER BY ManPhoneCode DESC LIMIT 1) as "mail",
        $fieldIsAsctive as "isActive",
        $fieldAge as "age"
        /* ulwp.Value as "workPlaceRem" */
$sqlFromWhere
order by $sort
limit $limit offset $start) AS t
EOT;
        $stmt = $db->prepare($sql);
        $stmt->execute();
        $root = new stdClass();
        $root->total = $total;
        $root->items = $stmt->fetchAll();
        foreach ($root->items as $key => $item) {
            if ($item['photoId'] != null) {
                $fileName = $fileName = "patients-photo/" . mb_substr($item['photoId'], 0, 1) . "/" . mb_substr($item['photoId'], 1, 2) . "/" . $item['photoId'] . ".jpg";
                if ($storage->isFileExists(2, $fileName)) {
                    $root->items[$key]['photoExist'] = 1;
                }
            }
        }
        return $root;
    }

    /**
     * Вставка/Обновление
     * @param type $records
     * @return boolean
     */
    public function insertOrUpdatePatients($records, $isInsert) {
        $db = $this->db ?: $this->db = connectDb();

        $enterpriseCode = getSessionValue('mapEnterpriseCode');
        $ret = [];

// Man
        $sqlMan = <<<"EOT"
INSERT INTO Man (ManCode, SurName, Name, SecondName)
VALUES (?, ?, ?, ?)
ON CONFLICT (ManCode) DO UPDATE SET
    SurName = case when ? = 1 then EXCLUDED.SurName else Man.SurName end,
    Name = case when ? = 1 then EXCLUDED.Name else Man.Name end,
    SecondName = case when ? = 1 then EXCLUDED.SecondName else Man.SecondName end
RETURNING ManCode AS id
EOT;
        $stmtMan = $db->prepare($sqlMan);

//ManPhone
        $sqlManPhone = <<<"EOT"
INSERT INTO ManPhone (ManPhoneCode, ManCode, PhoneNumber, PhoneType, Note)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT (ManPhoneCode) DO UPDATE SET
    PhoneNumber = case when ? = 1 then EXCLUDED.PhoneNumber else ManPhone.PhoneNumber end,
    Note = case when ? = 1 then EXCLUDED.Note else ManPhone.Note end
RETURNING ManPhoneCode AS id
EOT;
        $stmtManPhone = $db->prepare($sqlManPhone);

//ManPassport
        $sqlManPassport = <<<"EOT"
INSERT INTO ManPassport (
    ManPassportCode, ManCode, Sex, DateOfBirth, AddressCode, AddressResideCode, PassportNumber, Authority, DateOfIssue,
    DateOfExpiry, PlaceOfBirth, PersonalNumber, AddressHouse, AddressCorps, AddressFlat, Zipcode, AddressResideHouse,
    AddressResideCorps, AddressResideFlat, ZipcodeReside)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (ManPassportCode) DO UPDATE SET
    Sex = case when ? = 1 then EXCLUDED.Sex else ManPassport.Sex end,
    DateOfBirth = case when ? = 1 then EXCLUDED.DateOfBirth else ManPassport.DateOfBirth end,
    AddressCode = case when ? = 1 then EXCLUDED.AddressCode else ManPassport.AddressCode end,
    AddressResideCode = case when ? = 1 then EXCLUDED.AddressResideCode else ManPassport.AddressResideCode end,
    PassportNumber = case when ? = 1 then EXCLUDED.PassportNumber else ManPassport.PassportNumber end,
    Authority = case when ? = 1 then EXCLUDED.Authority else ManPassport.Authority end,
    DateOfIssue = case when ? = 1 then EXCLUDED.DateOfIssue else ManPassport.DateOfIssue end,
    DateOfExpiry = case when ? = 1 then EXCLUDED.DateOfExpiry else ManPassport.DateOfExpiry end,
    PlaceOfBirth = case when ? = 1 then EXCLUDED.PlaceOfBirth else ManPassport.PlaceOfBirth end,
    PersonalNumber = case when ? = 1 then EXCLUDED.PersonalNumber else ManPassport.PersonalNumber end,
    AddressHouse = case when ? = 1 then EXCLUDED.AddressHouse else ManPassport.AddressHouse end,
    AddressCorps = case when ? = 1 then EXCLUDED.AddressCorps else ManPassport.AddressCorps end,
    AddressFlat = case when ? = 1 then EXCLUDED.AddressFlat else ManPassport.AddressFlat end,
    Zipcode = case when ? = 1 then EXCLUDED.Zipcode else ManPassport.Zipcode end,
    AddressResideHouse = case when ? = 1 then EXCLUDED.AddressResideHouse else ManPassport.AddressResideHouse end,
    AddressResideCorps = case when ? = 1 then EXCLUDED.AddressResideCorps else ManPassport.AddressResideCorps end,
    AddressResideFlat = case when ? = 1 then EXCLUDED.AddressResideFlat else ManPassport.AddressResideFlat end,
    ZipcodeReside = case when ? = 1 then EXCLUDED.ZipcodeReside else ManPassport.ZipcodeReside end
RETURNING ManPassportCode AS id
EOT;
        $stmtManPassport = $db->prepare($sqlManPassport);

        $sqlMedPatient = <<<"EOT"
INSERT INTO MedPatient (MedPatientCode, Cipher, NumberHistorySickness, StartDate, FinishDate, MedPatientGroupCode,
    EnterpriseCode, MedPatientPolyclinicCode, ManCode, ArchivalNumber, OutReason)
VALUES (?, ?, ?, ?, COALESCE(?::date, 30000101)::date, ?, ?, ?, ?, ?, ?)
ON CONFLICT (MedPatientCode) DO UPDATE SET
    Cipher = case when ? = 1 then EXCLUDED.Cipher else MedPatient.Cipher end,
    NumberHistorySickness = case when ? = 1 then EXCLUDED.NumberHistorySickness else MedPatient.NumberHistorySickness end,
    StartDate = case when ? = 1 then EXCLUDED.StartDate else MedPatient.StartDate end,
    FinishDate = case when ? = 1 then EXCLUDED.FinishDate else MedPatient.FinishDate end,
    MedPatientGroupCode = case when ? = 1 then EXCLUDED.MedPatientGroupCode else MedPatient.MedPatientGroupCode end,
    MedPatientPolyclinicCode = case when ? = 1 then EXCLUDED.MedPatientPolyclinicCode else MedPatient.MedPatientPolyclinicCode end,
    ArchivalNumber = case when ? = 1 then EXCLUDED.ArchivalNumber else MedPatient.ArchivalNumber end,
    OutReason = case when ? = 1 then EXCLUDED.OutReason else MedPatient.OutReason end
RETURNING MedPatientCode AS id
EOT;
        $stmtMedPatient = $db->prepare($sqlMedPatient);

        if (is_object($records)) {
            $records = [$records];
        }
        $db->beginTransaction();
        try {
            foreach ($records as $row) {
                $ret[] = $rec = new stdClass;
// обновляем ФИО в Man если изменено
                $rec->manId = isUndefinedProperty($row, 'manId');
                if (isset($row->fio) && isUndefinedProperty($row, 'fio')) {
                    $aFio = trim($row->fio);
// парсим ФИО (последний пустой не обрабатывается, только первые три если заполнены)
                    $fio = preg_split("/[\s.,]+/u", $aFio);

                    $stmtMan->execute([
                        isUndefinedProperty($row, 'manId'),
                        $fio[0],
                        $fio[1],
                        trim(implode(" ", (array_slice($fio, 2)))),
                        isset($fio[0]) ? 1 : 0,
                        isset($fio[1]) ? 1 : 0,
                        isset($fio[2]) ? 1 : 0
                    ]);
                    $rec->manId = $stmtMan->fetch()['id'];
                    unset($fio);
                }

//ManPhone
// Домашний
                $stmtManPhone->execute([
                    isUndefinedProperty($row, 'phoneId'),
                    $rec->manId,
                    isUndefinedProperty($row, 'phone'),
                    1,
                    null,
                    propertyExistsSQL($row, 'phone'),
                    0
                ]);
                $rec->phoneId = $stmtManPhone->fetch()['id'];
// Мобильный
                $stmtManPhone->execute([
                    isUndefinedProperty($row, 'mobilePhoneId'),
                    $rec->manId,
                    isUndefinedProperty($row, 'mobilePhone'),
                    3,
                    isUndefinedProperty($row, 'mobilePhoneOperator'),
                    propertyExistsSQL($row, 'mobilePhone'),
                    propertyExistsSQL($row, 'mobilePhoneOperator')
                ]);
                $rec->mobilePhoneId = $stmtManPhone->fetch()['id'];
// Mail
                $stmtManPhone->execute([
                    isUndefinedProperty($row, 'mailId'),
                    $rec->manId,
                    isUndefinedProperty($row, 'mail'),
                    4,
                    null,
                    propertyExistsSQL($row, 'mail'),
                    0
                ]);
                $rec->mailId = $stmtManPhone->fetch()['id'];

                $oldAddress = $this->readAddress($row);
//ManPassport
                $stmtManPassport->execute([
                    isUndefinedProperty($row, 'manPassportId'),
                    $rec->manId,
                    isUndefinedProperty($row, 'sex'),
                    isUndefinedProperty($row, 'dateOfBirth'),
                    isUndefinedProperty($row, 'addressId'),
                    isUndefinedProperty($row, 'addressResideId'),
                    isUndefinedProperty($row, 'passportNumber'),
                    isUndefinedProperty($row, 'authority'),
                    isUndefinedProperty($row, 'dateOfIssue'),
                    isUndefinedProperty($row, 'dateOfExpiry'),
                    isUndefinedProperty($row, 'placeOfBirth'),
                    isUndefinedProperty($row, 'personalNumber'),
                    isUndefinedProperty($row, 'addressHouse'),
                    isUndefinedProperty($row, 'addressCorps'),
                    isUndefinedProperty($row, 'addressFlat'),
                    isUndefinedProperty($row, 'addressZipcode'),
                    isUndefinedProperty($row, 'addressResideHouse'),
                    isUndefinedProperty($row, 'addressResideCorps'),
                    isUndefinedProperty($row, 'addressResideFlat'),
                    isUndefinedProperty($row, 'addressResideZipcode'),
                    propertyExistsSQL($row, 'sex'),
                    propertyExistsSQL($row, 'dateOfBirth'),
                    propertyExistsSQL($row, 'addressId'),
                    propertyExistsSQL($row, 'addressResideId'),
                    propertyExistsSQL($row, 'passportNumber'),
                    propertyExistsSQL($row, 'authority'),
                    propertyExistsSQL($row, 'dateOfIssue'),
                    propertyExistsSQL($row, 'dateOfExpiry'),
                    propertyExistsSQL($row, 'placeOfBirth'),
                    propertyExistsSQL($row, 'personalNumber'),
                    propertyExistsSQL($row, 'addressHouse'),
                    propertyExistsSQL($row, 'addressCorps'),
                    propertyExistsSQL($row, 'addressFlat'),
                    propertyExistsSQL($row, 'addressZipcode'),
                    propertyExistsSQL($row, 'addressResideHouse'),
                    propertyExistsSQL($row, 'addressResideCorps'),
                    propertyExistsSQL($row, 'addressResideFlat'),
                    propertyExistsSQL($row, 'addressResideZipcode')
                ]);
                $rec->manPassportId = $stmtManPassport->fetch()['id'];

                $newAddress = $this->readAddress($row);
                if (strcasecmp($oldAddress, $newAddress)) {
                    $this->doWriteChangeAddress(isEmptyProperty($row, 'id', 0), $newAddress, $oldAddress);
                }
//MedPatient
                $stmtMedPatient->execute([
                    $isInsert ? null : isUndefinedProperty($row, 'id'),
                    isUndefinedProperty($row, 'cipher'),
                    isUndefinedProperty($row, 'numberHistorySickness'),
                    isUndefinedProperty($row, 'startDate'),
                    isUndefinedProperty($row, 'finishDate'),
                    isUndefinedProperty($row, 'sectorId'),
                    $enterpriseCode,
                    //isUndefinedProperty($row, 'ChangeDate'),
//isUndefinedProperty($row, 'LiveZone'),
                    isUndefinedProperty($row, 'clinicId'),
                    $rec->manId,
                    isUndefinedProperty($row, 'archivalNumber'),
                    isUndefinedProperty($row, 'outReason'),
                    propertyExistsSQL($row, 'cipher'),
                    propertyExistsSQL($row, 'numberHistorySickness'),
                    propertyExistsSQL($row, 'startDate'),
                    propertyExistsSQL($row, 'finishDate'),
                    propertyExistsSQL($row, 'sectorId'),
                    //propertyExistsSQL($row, 'ChangeDate'),
//propertyExistsSQL($row, 'LiveZone'),
                    propertyExistsSQL($row, 'clinicId'),
                    propertyExistsSQL($row, 'archivalNumber'),
                    propertyExistsSQL($row, 'outReason')
                ]);
                $rec->id = $stmtMedPatient->fetch()['id'];
            }
            $db->commit();
        } catch (Exception $e) {
            $db->rollback();
            throw $e;
        }

        return $ret;
    }

    /**
     * Чтение адреса
     * @param type $record
     * @return type
     */
    private function readAddress($record) {
        $db = $this->db ?: $this->db = connectDb();
        $sql = <<< "EOT"
        select FN_cmCalcAddress(?, 5)::text
EOT;
        $stmt = $db->prepare($sql);
        $stmt->execute([
            isEmptyProperty($record, 'manId', 0)
        ]);
        return $stmt->fetch()['fn_cmcalcaddress'];
    }

    /**
     * Запись изменений адреса
     * @param type $patientId
     * @param type $newAddress
     * @param type $oldAddress
     * @return type
     */
    private function doWriteChangeAddress($patientId, $newAddress, $oldAddress) {
        $db = $this->db ?: $this->db = connectDb();
        $currDate = udate('Ymd');
        $sql = <<< "EOT"
        INSERT INTO medPatientChange (medPatientCode, type, value, changeDate)
        VALUES (?, 1, ?, ?)
EOT;
        $stmt = $db->prepare($sql);
        $stmt->execute([
            $patientId,
            $newAddress . ' (' . $oldAddress . ')',
            $currDate
        ]);
        return $stmt->fetch();
    }

    /**
     * Вставка
     * @param type $records
     * @return \stdClass
     * @throws Exception
     */
    public function createPatients($records) {
        return $this->insertOrUpdatePatients($records, true);
    }

    /**
     * Обновление
     * @param type $records
     * @return boolean
     */
    public function updatePatients($records) {
        return $this->insertOrUpdatePatients($records, false);
    }

    /**
     * Удаление
     * @param type $records
     * @return boolean
     */
    public function deletePatients($records) {
        return /*
                  deleteSql($this->db, 'MedPatientBusy', 'MedPatientCode', $records) &&
                  deleteSql($this->db, 'MedPatientCard', 'MedPatientCode', $records) &&
                  deleteSql($this->db, 'MedBarCode', 'ManCode', $records, 'manId') &&
                 */
                deleteSql($this->db, 'MedPatient', 'MedPatientCode', $records);
    }

    /*
     * КЕМ ВЫДАН=======================================================================================================
     */

    /**
     * Функция получения списка "кем выдан" для посказки
     * @param type $params
     * @return [{obj}] список
     */
    public function readPassportAuthoritys($params) {
        $db = $this->db ?: $this->db = connectDb();
        $find = isUndefinedProperty($params, 'find');
        if ($find) {
            $query = mb_ereg_replace("[\s]+", ":* & ", trim($find)) . ":*";
            $sql = <<<"EOT"
SELECT DISTINCT asa_trim(Authority) as "id"
FROM ManPassport
WHERE Authority IS NOT NULL
  AND Authority <> ''
  AND to_tsvector(Authority) @@ to_tsquery('{$query}')
--GROUP BY upper(asa_trim(Authority)), "id"
ORDER BY 1
EOT;
            $stmt = $db->prepare($sql);
            $stmt->execute();
            return $stmt->fetchAll();
        } else {
            return [];
        }
    }

    /*
     * СПЕЦУЧАСТКИ======================================================================================================
     */

    /**
     * Функция получения списка спецучастков по пациенту
     * @param type $params
     *  patientId (int) patientId
     * @return [{obj}] список спецучасков пациента
     */
    public function readAddSectors($params) {
        $db = $this->db ?: $this->db = connectDb();
        $patientId = isEmptyProperty($params, 'patientId', null);
        $sql = <<<"EOT"
SELECT
    mpag.MedPatientGroupCode as "id",
    mpag.MedPatientAddGroupCode as "sectorAddId",
    mpag.MedPatientCode as "patientId"
FROM MedPatientAddGroup mpag
LEFT JOIN MedPatientGroup AS mpg ON mpg.MedPatientGroupCode = mpag.MedPatientGroupCode
WHERE mpag.MedPatientCode = $patientId
ORDER BY mpg.Name
EOT;
        $stmt = $db->prepare($sql);
        $stmt->execute();
        return $stmt->fetchAll();
    }

    /**
     * Вставка спецучастков
     * @param type $records
     * @return \stdClass
     * @throws Exception
     */
    public function createAddSectors($records) {
        $db = $this->db ?: $this->db = connectDb();
        $ret = [];
        $sql = <<<"EOT"
INSERT INTO MedPatientAddGroup (MedPatientCode, MedPatientGroupCode)
VALUES (?, ?)
EOT;
        $stmt = $db->prepare($sql);
        if (is_object($records)) {
            $records = [$records];
        }
        $db->beginTransaction();
        try {
            foreach ($records as $row) {
                $ret[] = $rec = new stdClass;
                $stmt->execute([
                    isUndefinedProperty($row, 'patientId'),
                    isUndefinedProperty($row, 'id')
                ]);
                $rec->sectorAddId = $db->lastInsertId('seq_MedPatientAddGroup');
            }
            $db->commit();
        } catch (Exception $e) {
            $db->rollback();
            throw $e;
        }
        return $ret;
    }

    /**
     * Удаление спецучастков
     * @param type $records
     * @return boolean
     */
    public function deleteAddSectors($records) {
        return deleteSql($this->db, 'MedPatientAddGroup', 'MedPatientAddGroupCode', $records, 'sectorAddId');
    }

    /*
     * ЗАНЯТОСТЬ========================================================================================================
     */

    /**
     * Функция получения списка занятости по пациенту
     * @param type $params
     *  patientId (int) patientId
     *  currBusy (bool) true - на текущую дату(для выборки из других форм), false - все
     * @return type
     */
    public function readPatientBusy($params) {
        $db = $this->db ?: $this->db = connectDb();
        $patientId = isEmptyProperty($params, 'patientId', null);
        $currDate = Date('Ymd');
        $currBusy = isset($params->currBusy) && $params->currBusy ? "AND $currDate BETWEEN mb.StartDate AND mb.FinishDate" : "";
        $sql = <<<"EOT"
SELECT
    mb.MedPatientBusyCode as "id",
    mb.MedPatientCode as "patientId",
    mb.StartDate as "startDate",
    CASE
        WHEN COALESCE(mb.FinishDate, 30000101)::date = 30000101 THEN null
        ELSE mb.FinishDate
    END as "finishDate",
    mb.EnterpriseKindCode as "organizationKindId",
    mb.EnterpriseCode as "organizationId",
    e.Name as "organizationName",
    mb.ClassGroup as "classGroup",
    mb.UniDictCodeProfession as "professionId",
    (select Value from UniDict where uniDictCode = mb.UniDictCodeProfession) as "professionName",
    mb.DepartmentCode as "departmentId",
    FN_cmDepartmentName(mb.DepartmentCode) as "departmentName"
FROM MedPatientBusy mb
LEFT JOIN Enterprise as e ON mb.EnterpriseCode = E.EnterpriseCode
WHERE mb.MedPatientCode = $patientId
  $currBusy
ORDER BY "startDate" DESC, "finishDate", "id" DESC
EOT;
        $stmt = $db->prepare($sql);
        $stmt->execute();
        return $stmt->fetchAll();
    }

    /**
     * Вставка/обновление занятости
     * @param type $records
     * @return array
     */
    public function insertOrUpdatePatientBusy($records, $isInsert) {
        $db = $this->db ?: $this->db = connectDb();
        $ret = [];
        $sql = <<<"EOT"
INSERT INTO MedPatientBusy (
    MedPatientBusyCode, MedPatientCode, StartDate, FinishDate, EnterpriseKindCode, EnterpriseCode, DepartmentCode,
    ClassGroup, UniDictCodeProfession)
VALUES (?, ?, ?, COALESCE(?::date, 30000101)::date, ?, ?, ?, ?, ?)
ON CONFLICT (MedPatientBusyCode) DO UPDATE SET
    StartDate = case when ? = 1 then EXCLUDED.StartDate else MedPatientBusy.StartDate end,
    FinishDate = case when ? = 1 then EXCLUDED.FinishDate else MedPatientBusy.FinishDate end,
    EnterpriseKindCode = case when ? = 1 then EXCLUDED.EnterpriseKindCode else MedPatientBusy.EnterpriseKindCode end,
    EnterpriseCode = case when ? = 1 then EXCLUDED.EnterpriseCode else MedPatientBusy.EnterpriseCode end,
    DepartmentCode = case when ? = 1 then EXCLUDED.DepartmentCode else MedPatientBusy.DepartmentCode end,
    ClassGroup = case when ? = 1 then EXCLUDED.ClassGroup else MedPatientBusy.ClassGroup end,
    UniDictCodeProfession = case when ? = 1 then EXCLUDED.UniDictCodeProfession else MedPatientBusy.UniDictCodeProfession end
RETURNING MedPatientBusyCode AS id
EOT;
        $stmt = $db->prepare($sql);

        if (is_object($records)) {
            $records = [$records];
        }
        $db->beginTransaction();
        try {
            foreach ($records as $row) {
                $ret[] = $rec = new stdClass;
                $stmt->execute([
                    $isInsert ? null : isUndefinedProperty($row, 'id'),
                    isUndefinedProperty($row, 'patientId'),
                    isUndefinedProperty($row, 'startDate'),
                    isUndefinedProperty($row, 'finishDate'),
                    isUndefinedProperty($row, 'organizationKindId'),
                    isUndefinedProperty($row, 'organizationId'),
                    isUndefinedProperty($row, 'departmentId'),
                    isUndefinedProperty($row, 'classGroup'),
                    isUndefinedProperty($row, 'professionId'),
                    propertyExistsSQL($row, 'startDate'),
                    propertyExistsSQL($row, 'finishDate'),
                    propertyExistsSQL($row, 'organizationKindId'),
                    propertyExistsSQL($row, 'organizationId'),
                    propertyExistsSQL($row, 'departmentId'),
                    propertyExistsSQL($row, 'classGroup'),
                    propertyExistsSQL($row, 'professionId')
                ]);
                $rec->id = $stmt->fetch()['id'];
            }
            $db->commit();
        } catch (Exception $e) {
            $db->rollback();
            throw $e;
        }

        return $ret;
    }

    /**
     * Вставка занятости
     * @param type $records
     * @return \stdClass
     * @throws Exception
     */
    public function createPatientBusy($records) {
        return $this->insertOrUpdatePatientBusy($records, true);
    }

    /**
     * Обновление занятости
     * @param type $records
     * @return boolean
     */
    public function updatePatientBusy($records) {
        return $this->insertOrUpdatePatientBusy($records, false);
    }

    /**
     * Удаление занятости
     * @param type $records
     * @return boolean
     */
    public function deletePatientBusy($records) {
        return deleteSql($this->db, 'MedPatientBusy', 'MedPatientBusyCode', $records);
    }

    /*
     * ПРИНАДЛЕЖНОСТЬ К КОНТИНГЕНТАМ====================================================================================
     */

    /**
     * Функция получения списка принадлежности к контигентам по пациенту
     * @param {object} $params
     * patientId (int) - код пациента
     * currContingents (bool) true - на текущую дату(для выборки из других форм), false - все
     * isPrivelege (bool) true - для выбора только по связке с MedPrivelege c PrivilegePercent > 0.0
     * @return type
     */
    public function readPatientContingents($params) {
        $db = $this->db ?: $this->db = connectDb();
        $patientId = isEmptyProperty($params, 'patientId', null);
        $currDate = Date('Ymd');
        $currContingents = isset($params->currContingents) && $params->currContingents ? "AND $currDate BETWEEN PC.StartDate AND PC.FinishDate" : "";
        $isPrivilege = isset($params->isPrivilege) && $params->isPrivilege ? "AND COALESCE(P.PrivilegePercent, 0.0) > 0.0" : "";
        $sql = <<<"EOT"

SELECT PC.MedPatientCardCode AS "id",
       PC.MedPatientCode AS "patientId",
       PC.StartDate AS "startDate",
       CASE
           WHEN COALESCE(PC.FinishDate, 30000101)::date = 30000101 THEN null
           ELSE PC.FinishDate
       END as "finishDate",
       CASE
           WHEN COALESCE(PC.RebillingDate, 30000101)::date = 30000101 THEN null
           ELSE PC.RebillingDate
       END as "rebillingDate",
       PC.MedPrivelegeCode AS "contingentId",
       P.Name AS "contingentName",
       PC.MedDiagnosisCode AS "diagnosisId",
       PC.Document AS "document",
       PC.DocumentNumber AS "documentNumber",
       PC.DocumentSeries AS "documentSeries",
       PC.DocumentDate AS "documentDate",
       PC.Enterprise AS "enterprise",
/*       asa_substr(D.Name + (CASE
                                WHEN NullIf(asa_trim(D.Cipher1), '') IS NULL THEN ''
                                ELSE ' - ' + asa_trim(D.Cipher1)
                            END), 1, 255) AS "diagnosisName",
*/
       CASE P.MedPrivelegeCode IS NOT NULL
           WHEN TRUE THEN P.PrivilegePercent
           WHEN FALSE THEN D.PrivilegePercent
       END AS "privilegePercent",
       FN_cmGetNumberBitList(PC.CauseDisability) AS "causeDisability",
       P.MedPrivilegeGroupCode as "privilegeGroupId"
FROM MedPatientCard AS PC
LEFT JOIN MedDiagnosis AS D ON D.MedDiagnosisCode = PC.MedDiagnosisCode
LEFT JOIN MedPrivelege AS P ON P.MedPrivelegeCode = PC.MedPrivelegeCode
WHERE PC.MedPatientCode = $patientId
  $currContingents
  $isPrivilege
ORDER BY "startDate" DESC, "finishDate", "id" DESC
EOT;
        $stmt = $db->prepare($sql);
        $stmt->execute();
        return $stmt->fetchAll();
    }

    /**
     * Вставка/обновление принадлежности к контигентам
     * @param type $records
     * @return array
     */
    public function insertOrUpdatePatientContingents($records, $isInsert) {
        $db = $this->db ?: $this->db = connectDb();
        $ret = [];
        $sql = <<<"EOT"
INSERT INTO MedPatientCard (
    MedPatientCardCode, MedPatientCode, StartDate, FinishDate, RebillingDate, MedPrivelegeCode, MedDiagnosisCode,
    Document, DocumentNumber, DocumentSeries, DocumentDate, Enterprise, CauseDisability)
VALUES (?, ?, ?, COALESCE(?::date, 30000101)::date, COALESCE(?::date, 30000101)::date, ?, ?, ?, ?, ?, ?, ?, FN_cmGetBitListNumber(?))
ON CONFLICT (MedPatientCardCode) DO UPDATE SET
    StartDate = case when ? = 1 then EXCLUDED.StartDate else MedPatientCard.StartDate end,
    FinishDate = case when ? = 1 then EXCLUDED.FinishDate else MedPatientCard.FinishDate end,
    RebillingDate = case when ? = 1 then EXCLUDED.RebillingDate else MedPatientCard.RebillingDate end,
    MedPrivelegeCode = case when ? = 1 then EXCLUDED.MedPrivelegeCode else MedPatientCard.MedPrivelegeCode end,
    MedDiagnosisCode = case when ? = 1 then EXCLUDED.MedDiagnosisCode else MedPatientCard.MedDiagnosisCode end,
    Document = case when ? = 1 then EXCLUDED.Document else MedPatientCard.Document end,
    DocumentNumber = case when ? = 1 then EXCLUDED.DocumentNumber else MedPatientCard.DocumentNumber end,
    DocumentSeries = case when ? = 1 then EXCLUDED.DocumentSeries else MedPatientCard.DocumentSeries end,
    DocumentDate = case when ? = 1 then EXCLUDED.DocumentDate else MedPatientCard.DocumentDate end,
    Enterprise = case when ? = 1 then EXCLUDED.Enterprise else MedPatientCard.Enterprise end,
    CauseDisability = case when ? = 1 then EXCLUDED.CauseDisability else MedPatientCard.CauseDisability end
RETURNING MedPatientCardCode AS id
EOT;
        $stmt = $db->prepare($sql);

        if (is_object($records)) {
            $records = [$records];
        }
        $db->beginTransaction();
        try {
            foreach ($records as $row) {
                $ret[] = $rec = new stdClass;
                $stmt->execute([
                    $isInsert ? null : isUndefinedProperty($row, 'id'),
                    isUndefinedProperty($row, 'patientId'),
                    isUndefinedProperty($row, 'startDate'),
                    isUndefinedProperty($row, 'finishDate'),
                    isUndefinedProperty($row, 'rebillingDate'),
                    isUndefinedProperty($row, 'contingentId'),
                    isUndefinedProperty($row, 'diagnosisId'),
                    isUndefinedProperty($row, 'document'),
                    isUndefinedProperty($row, 'documentNumber'),
                    isUndefinedProperty($row, 'documentSeries'),
                    isUndefinedProperty($row, 'documentDate'),
                    isUndefinedProperty($row, 'enterprise'),
                    isUndefinedProperty($row, 'causeDisability'),
                    propertyExistsSQL($row, 'startDate'),
                    propertyExistsSQL($row, 'finishDate'),
                    propertyExistsSQL($row, 'rebillingDate'),
                    propertyExistsSQL($row, 'contingentId'),
                    propertyExistsSQL($row, 'diagnosisId'),
                    propertyExistsSQL($row, 'document'),
                    propertyExistsSQL($row, 'documentNumber'),
                    propertyExistsSQL($row, 'documentSeries'),
                    propertyExistsSQL($row, 'documentDate'),
                    propertyExistsSQL($row, 'enterprise'),
                    propertyExistsSQL($row, 'causeDisability')
                ]);
                $rec->id = $stmt->fetch()['id'];
            }
            $db->commit();
        } catch (Exception $e) {
            $db->rollback();
            throw $e;
        }

        return $ret;
    }

    /**
     * Вставка принадлежности к контигентам
     * @param type $records
     * @return \stdClass
     * @throws Exception
     */
    public function createPatientContingents($records) {
        return $this->insertOrUpdatePatientContingents($records, true);
    }

    /**
     * Обновление принадлежности к контигентам
     * @param type $records
     * @return boolean
     */
    public function updatePatientContingents($records) {
        return $this->insertOrUpdatePatientContingents($records, false);
    }

    /**
     * Удаление принадлежности к контигентам
     * @param type $records
     * @return boolean
     */
    public function deletePatientContingents($records) {
        return deleteSql($this->db, 'MedPatientCard', 'MedPatientCardCode', $records);
    }

    /*
     * КАРТОЧКИ=========================================================================================================
     */

    /**
     * Функция получения списка индивидуальных карточек
     * @param type $params
     * @return type
     */
    public function readPatientBarCodes($params) {
        $db = $this->db ?: $this->db = connectDb();
        $patientId = isEmptyProperty($params, 'patientId', null);
        $sql = <<<"EOT"
SELECT mbc.BarCode AS "id",
       mbc.BarCode AS "barCode",
       mbc.StartDate AS "startDate",
       mbc.StopDate AS "stopDate",
       mbc.ManCode AS "manId",
       /* mbc.UserCode AS "userCode", */
       mbc.State AS "state"
FROM MedPatient AS mp
JOIN MedBarCode mbc ON mbc.ManCode = mp.ManCode
WHERE mp.MedPatientCode = $patientId
ORDER BY "startDate" DESC, "stopDate", "id" DESC
EOT;
        $stmt = $db->prepare($sql);
        $stmt->execute();
        return $stmt->fetchAll();
    }

    /**
     * Функция добавления индивидуальных карточек
     * @param type $records
     * @return type
     */
    public function createPatientBarCodes($records) {
        $db = $this->db ?: $this->db = connectDb();
        $mapUserCode = getSessionValue('mapUserCode');
        $ret = [];

        /* Обновление старых карточек
         * $stopDate = date_add(isUndefinedProperty($row, 'startDate'), date_interval_create_from_date_string('-1 days'));
          $sqlUpdateOld = <<<"EOT"
          UPDATE MedBarCode SET
          StopDate = $stopDate
          WHERE ManCode = ?
          AND StopDate IS NULL
          EOT;
          $stmtUpdateOld = $db->prepare($sqlUpdateOld);
         */
        $sql = <<<"EOT"
INSERT INTO MedBarCode (BarCode, StartDate, StopDate, ManCode, UserCode, State)
VALUES (?, ?, ?, ?, $mapUserCode, 0)
EOT;
        $stmt = $db->prepare($sql);
        if (is_object($records)) {
            $records = [$records];
        }
        $db->beginTransaction();
        try {
            foreach ($records as $row) {
                $ret[] = $rec = new stdClass;
                /*
                  $stmtUpdateOld->execute([
                  isUndefinedProperty($row, 'manId')
                  ]);
                 */
                $stmt->execute([
                    isUndefinedProperty($row, 'barCode'),
                    isUndefinedProperty($row, 'startDate'),
                    isUndefinedProperty($row, 'stopDate'),
                    isUndefinedProperty($row, 'manId')
                ]);
                $rec->id = isUndefinedProperty($row, 'barCode');
            }
            $db->commit();
        } catch (Exception $e) {
            $db->rollback();
            throw $e;
        }

        return $ret;
    }

    /**
     * Функция обновления индивидуальных карточек
     * @param type $records
     * @return type
     */
    public function updatePatientBarCodes($records) {
        $db = $this->db ?: $this->db = connectDb();
        $mapUserCode = getSessionValue('mapUserCode');
        $ret = [];
        $sql = <<<"EOT"
UPDATE MedBarCode SET
    BarCode = case when ? = 1 then ? else BarCode end,
    StartDate = case when ? = 1 then ? else StartDate end,
    StopDate = case when ? = 1 then ? else StopDate end
WHERE BarCode = ?
EOT;
        $stmt = $db->prepare($sql);
        if (is_object($records)) {
            $records = [$records];
        }
        $db->beginTransaction();
        try {
            foreach ($records as $row) {
                $ret[] = $rec = new stdClass;
                $stmt->execute([
                    propertyExistsSQL($row, 'barCode'),
                    isUndefinedProperty($row, 'barCode'),
                    propertyExistsSQL($row, 'startDate'),
                    isUndefinedProperty($row, 'startDate'),
                    propertyExistsSQL($row, 'stopDate'),
                    isUndefinedProperty($row, 'stopDate'),
                    isUndefinedProperty($row, 'id')
                ]);
                $rec->id = isUndefinedProperty($row, 'barCode');
            }
            $db->commit();
        } catch (Exception $e) {
            $db->rollback();
            throw $e;
        }

        return $ret;
    }

    /**
     * Удаление индивидуальной карточки
     * @param type $records
     * @return boolean
     */
    public function deletePatientBarCodes($records) {
        return deleteSql($this->db, 'MedBarCode', 'BarCode', $records);
    }

    /**
     * Чтение информации по пациенту
     * @param object $params
     *  patientId (int) - код пациента
     *  isBusy (bool) - если нужно выбрать занятость
     *  isContingents (bool) - если нужно выбрать контингенты
     *  getFio (bool) - ФИО
     *  getAge (bool) - Дата рождения, кол-во лет (dateOfBirth, age(interval))
     *  getAddress (bool) - адрес
     *  getSector (bool) - участок (sectorName, sectorId)
     *  getVisitZone (bool) - зона обслуживания
     *  getCardNumber (bool) - номер картыы
     * @return object
     *  patientFio (string) - фио пациента
     *  age
     *  dateOfBirth
     *  sectorName
     *  sectorId
     *  visitZone
     *  cardNumber
     *  busy {object}
     *  contingents {object}
     */
    public function readPatientInfo($params) {
        $db = $this->db ?: $this->db = connectDb();

        $patientId = isEmptyProperty($params, 'patientId', 0);

        $getAge = isEmptyProperty($params, 'getAge');
        $getAddress = isEmptyProperty($params, 'getAddress');
        $getSector = isEmptyProperty($params, 'getSector');
        $getVisitZone = isEmptyProperty($params, 'getVisitZone');
        // проверка переданных флагов
        // поля
        $fieldFio = isUndefinedProperty($params, 'getFio', true) ? "FN_cmFIO(MP.manCode) \"patientFio\"" : "1";
        $fieldsAge = $getAge ?
                ", FN_wgCalcStage(2, CURRENT_DATE, MPASS.DateOfBirth, 0, 0, 0, null) \"age\""
                . ", MPASS.DateOfBirth \"dateOfBirth\"" : "";
        $fieldAddress = $getAddress ? ", FN_cmCalcAddress(MP.ManCode, 5, 0, 1) \"address\"" : "";
        $fieldsSector = $getSector ? ", VISIT_ZONE.Name \"sectorName\""
                . ", VISIT_ZONE.MedPatientGroupCode \"sectorId\"" : "";
        $fieldVisitZone = $getVisitZone ? ", VISIT_ZONE.VisitZone \"visitZone\"" : "";
        $fieldCardNumber = isEmptyProperty($params, 'getCardNumber') ? ", MP.NumberHistorySickness \"cardNumber\"" : "";
        // таблицы
        // проверяем на необходимость подключить таблицу manPassport
        $joinMPASS = $getAge || $getAddress ?
                "left join ManPassport MPASS on MPASS.ManCode = MP.ManCode" : "";
        $joinSector = $getSector || $getVisitZone ?
                "left join lateral (select
                                               _VISIT_ZONE.VisitZone,
                                               _VISIT_ZONE.MedPatientGroupCode,
                                               _PGN.Name
                                        from (select
                                                     _T.MedPatientGroupCode,
                                                     _T.VisitZone
                                              from (select
                                                           _AG.MedPatientGroupCode,
                                                           _PG.VisitZone
                                                    from MedPatientAddGroup _AG
                                                         left join medPatientGroup _PG on _PG.medPatientGroupCode = _AG.MedPatientGroupCode
                                                    where _AG.MedPatientCode = MP.medPatientCode
                                                    union
                                                    select
                                                           _MP.MedPatientGroupCode,
                                                           _PG.VisitZone
                                                    from MedPatient _MP
                                                         left join medPatientGroup _PG on _PG.medPatientGroupCode = _MP.MedPatientGroupCode
                                                    where _MP.MedPatientCode = MP.medPatientCode
                                                    order by 2 desc, 1 desc) AS _T
                                              limit 1) _VISIT_ZONE
                                             left join medPatientGroup _PGN on _PGN.medPatientGroupCode = _VISIT_ZONE.MedPatientGroupCode) VISIT_ZONE on true" : "";

        $sql = <<<"EOT"
select
       $fieldFio
       $fieldsAge
       $fieldAddress
       $fieldsSector
       $fieldVisitZone
       $fieldCardNumber
from MedPatient MP
     $joinMPASS
     $joinSector
where MP.MedPatientCode = $patientId
EOT;
        $stmt = $db->prepare($sql);
        //error_log(print_r($stmt, true));
        $stmt->execute();
        $ret = $stmt->fetch();

        if (isEmptyProperty($params, 'isBusy')) {// если нужно выбрать занятость
            $par = new stdClass();
            $par->patientId = $patientId;
            $par->currBusy = true; // грузим только текущую
            $ret{'busy'} = $this->readPatientBusy($par);
        }
        if (isEmptyProperty($params, 'isContingents')) { // если нужно загрузить льготы пациента
            $par = new stdClass();
            $par->patientId = $patientId;
            $par->currContingents = true; // грузим только текущую
            $ret{'contingents'} = $this->readPatientContingents($par);
        }
        return $ret;
    }

    /**
     * Получение списка заболеваний пациента
     * @param type $params
     * @return type
     */
    public function readPatientDeseases($params) {
        $db = $this->db ?: $this->db = connectDb();
        $sqlDeseases = <<<"EOT"
select
    mpd.MedPatientDeseasesCode "id",
    mpd.MedDiagnosisCode "diagnosisId",
    md.Cipher1 "cipher",
    md.Name "name",
    mpd.DateDeseases "dateDeseases",
    mpd.MedPatientCode "patientCode"
from MedPatient mp
    join MedPatientDeseases mpd on mpd.MedPatientCode = mp.MedPatientCode
    left join MedDiagnosis AS md on md.MedDiagnosisCode = mpd.MedDiagnosisCode
where mp.MedPatientCode = ?
order by mpd.DateDeseases desc
EOT;
        $stmtDeseases = $db->prepare($sqlDeseases);
        $stmtDeseases->execute([$params->patientCode]);
        return $stmtDeseases->fetchAll();
    }

    /**
     * Создание заболевания
     * @param type $records
     * @return \stdClass
     * @throws Exception
     */
    public function createPatientDeseases($records) {
        $db = $this->db ?: $this->db = connectDb();

        $sql = <<<"EOT"
insert into MedPatientDeseases (MedDiagnosisCode, DateDeseases, MedPatientCode)
values (?, ?, ?)
EOT;
        $stmt = $db->prepare($sql);
        if (is_object($records)) {
            $records = [$records];
        }
        $ret = [];
        $db->beginTransaction();
        try {
            foreach ($records as $row) {
                $stmt->execute([
                    isUndefinedProperty($row, 'diagnosisId'),
                    isUndefinedProperty($row, 'dateDeseases'),
                    isUndefinedProperty($row, 'patientCode')
                ]);
                $ret[] = $rec = new stdClass;
                $rec->id = $db->lastInsertId('seq_MedPatientDeseases');
            }
            $db->commit();
        } catch (Exception $e) {
            $db->rollback();
            throw $e;
        }

        return $ret;
    }

    /**
     * Обновление заболевания
     * @param type $records
     * @return boolean
     */
    public function updatePatientDeseases($records) {
        $db = $this->db ?: $this->db = connectDb();

        $sql = <<<"EOT"
update MedPatientDeseases set
       MedDiagnosisCode = case when ? = 1 then ? else MedDiagnosisCode end,
       DateDeseases = case when ? = 1 then ? else DateDeseases end
where MedPatientDeseasesCode = ?
EOT;
        $stmt = $db->prepare($sql);
        if (is_object($records)) {
            $records = [$records];
        }
        $db->beginTransaction();
        try {
            foreach ($records as $row) {
                $stmt->execute([
                    propertyExistsSQL($row, 'diagnosisId'), isUndefinedProperty($row, 'diagnosisId'),
                    propertyExistsSQL($row, 'dateDeseases'), isUndefinedProperty($row, 'dateDeseases'),
                    $row->id
                ]);
            }
            $db->commit();
        } catch (Exception $e) {
            $db->rollback();
            throw $e;
        }

        return true;
    }

    /**
     * Удаление заболевания
     * @param type $records
     * @return boolean
     */
    public function deletePatientDeseases($records) {
        $db = $this->db ?: $this->db = connectDb();
        $in = createSqlInCondition($records);
        $sql = <<<"EOT"
delete from MedPatientDeseases where MedPatientDeseasesCode $in
EOT;
        $stmt = $db->prepare($sql);
        $db->beginTransaction();

        try {
            $stmt->execute();
            $db->commit();
        } catch (Exception $e) {
            $db->rollback();
            throw $e;
        }

        return true;
    }

}

/* т.к. $notExecute используется тут для ограничения запуска других сервисов вызываемых отсюда,
 * то тут проверка для диспонсеризации
 */
//if (!isset($dispNotExecute)) {
//    $service = new Junior\Server(new med_common_patients());
//    $service->process();
//}
handleJsonRpc('med_common_patients');

​
