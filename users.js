// ==========================================
// USER AUTHENTICATION & MAPPING
// ==========================================

const RAW_USER_DATA = {
  "BATANGAS": [
    "PESO AGONCILLO", "PESO ALITAGTAG", "PESO BALAYAN", "PESO BALETE", "PESO BATANGAS CITY",
    "PESO BAUAN", "PESO CALACA", "PESO CALATAGAN", "PESO CUENCA", "PESO IBAAN", "PESO LAUREL",
    "PESO LEMERY", "PESO LIAN", "PESO LIPA CITY", "PESO LOBO", "PESO MABINI", "PESO MALVAR",
    "PESO MATAASNAKAHOY", "PESO NASUGBU", "PESO PADRE GARCIA", "PESO ROSARIO", "PESO SAN JOSE",
    "PESO SAN JUAN", "PESO SAN LUIS", "PESO SAN NICOLAS", "PESO SAN PASCUAL", "PESO SANTA TERESITA",
    "PESO SANTO TOMAS", "PESO TAAL", "PESO TALISAY", "PESO TANAUAN CITY", "PESO TAYSAN",
    "PESO TINGLOY", "PESO TUY", "PESO BATANGAS", "BATANGAS STATE UNIVERSITY",
    "LYCEUM OF THE PHILIPPINES UNIVERSITY-BATANGAS", "UNIVERSITY OF BATANGAS-MAIN",
    "RIZAL COLLEGE OF TAAL", "STI COLLEGE BACOOR", "NATIONAL UNIVERSITY-DASMARIÑAS CITY CAMPUS",
    "TAGAYTAY TOURISM COUNCIL", "OLIVAREZ COLLEGE-TAGAYTAY INC", "MEDICAL CENTER WESTERN BATANGAS",
    "LIMA ESTATE (LIMA LAND, INC.)", "CIVIL SERVICE COMMISSION FIELD OFFICE BATANGAS",
    "TECHNICAL EDUCATION AND SKILLS DEVELOPMENT AUTHORITY"
  ],

  "CAVITE": [
    "PESO ALFONSO", "PESO AMADEO", "PESO BACOOR CITY", "PESO CARMONA", "PESO CAVITE CITY",
    "PESO DASMARIÑAS CITY", "PESO GEA", "PESO GMA", "PESO CITY OF GENERAL TRIAS",
    "PESO IMUS CITY", "PESO INDANG", "PESO KAWIT", "PESO MAGALLANES", "PESO MARAGONDON",
    "PESO MENDEZ", "PESO NAIC", "PESO NOVELETA", "PESO ROSARIO", "PESO SILANG",
    "PESO TAGAYTAY CITY", "PESO TANZA", "PESO TERNATE", "PESO TRECE MARTIRES CITY",
    "PESO CAVITE PROVINCE"
  ],

  "LAGUNA": [
    "PESO ALAMINOS", "PESO BAY", "PESO CITY OF BIÑAN", "PESO CABUYAO CITY",
    "PESO CITY OF CALAMBA", "PESO CALAUAN", "PESO CAVINTI", "PESO FAMY", "PESO KALAYAAN",
    "PESO LILIW", "PESO LOS BAÑOS", "PESO LUISIANA", "PESO LUMBAN", "PESO MABITAC",
    "PESO MAGDALENA", "PESO MAJAYJAY", "PESO NAGCARLAN", "PESO PAETE", "PESO PAGSANJAN",
    "PESO PAKIL", "PESO PANGIL", "PESO PILA", "PESO RIZAL", "OPCR", "PESO SAN PABLO CITY",
    "PESO SAN PEDRO CITY", "PESO SANTA CRUZ", "PESO SANTA MARIA", "PESO CITY OF SANTA ROSA",
    "PESO SINILOAN", "PESO VICTORIA", "PESO LAGUNA", "PAUL", "FRANCIS", "FRANCE", "KEANNU", "JM", "MADZ", "RIRI",
    "LAGUNA STATE POLYTECHNIC UNIVERSITY-MAIN"
  ],

  "QUEZON": [
    "PESO AGDANGAN", "PESO ALABAT", "PESO ATIMONAN", "PESO BUENAVISTA", "PESO BURDEOS",
    "PESO CALAUAG", "PESO CANDELARIA", "PESO CATANAUAN", "PESO DOLORES", "PESO GENERAL LUNA",
    "PESO GENERAL NAKAR", "PESO GUINAYANGAN", "PESO GUMACA", "PESO INFANTA", "PESO JOMALIG",
    "PESO LOPEZ", "PESO LUCBAN", "PESO LUCENA CITY", "PESO MACALELON", "PESO MAUBAN",
    "PESO MULANAY", "PESO PADRE BURGOS", "PESO PAGBILAO", "PESO PANUKULAN", "PESO PATNANUNGAN",
    "PESO PEREZ", "PESO PITOGO", "PESO PLARIDEL", "PESO POLILLO", "PESO QUEZON", "PESO REAL",
    "PESO SAMPALOC", "PESO SAN ANDRES", "PESO SAN ANTONIO", "PESO SAN FRANCISCO",
    "PESO SAN NARCISO", "PESO SARIAYA", "PESO TAGKAWAYAN", "PESO TAYABAS CITY",
    "PESO TIAONG", "PESO UNISAN", "PESO QUEZON",
    "ACEBA SCIENCE AND TECHNOLOGY INSTITUTE-MAUBAN",
    "SOUTHERN LUZON STATE UNIVERSITY-LUCBAN"
  ],

  "RIZAL": [
    "PESO ANGONO", "PESO CITY OF ANTIPOLO", "PESO BARAS", "PESO BINANGONAN", "PESO CAINTA",
    "PESO CARDONA", "PESO JALAJALA", "PESO MORONG", "PESO PILILLA", "PESO RODRIGUEZ",
    "PESO SANMATEO", "PESO TANAY", "PESO TAYTAY", "PESO TERESA", "PESO RIZAL"
  ]
};


// ==========================================
// USER MAPS
// ==========================================

const usersMap = {};
const PROVINCE_MAP = {};


// ==========================================
// PASSWORD GENERATION
// ==========================================
// Removes ALL spaces from username before creating password.
//
// Example:
// SANTA ROSA  -> SANTAROSA@2026
// SANTO TOMAS -> SANTOTOMAS@2026
//
// Other users also follow the same rule:
// SAN JOSE -> SANJOSE@2026
// LIPA CITY -> LIPACITY@2026
// ==========================================

function generateUserPassword(cleanName) {
  return String(cleanName || '')
    .replace(/\s+/g, '')
    .toUpperCase() + "@2026";
}


// ==========================================
// BUILD USER MAP
// ==========================================

for (const province in RAW_USER_DATA) {

  PROVINCE_MAP[province] = {};

  RAW_USER_DATA[province].forEach(item => {

    const cleanName = item
      .replace(/^PESO\s+/i, '')
      .replace(/^CITY\s+OF\s+/i, '')
      .replace(/\s+CITY$/i, '')
      .trim()
      .toUpperCase();

    // Remove spaces ONLY from the password.
    // Username remains normalized with spaces.
    usersMap[cleanName] = generateUserPassword(cleanName);

    PROVINCE_MAP[province][cleanName] = true;
  });
}


// ==========================================
// SPECIAL PASSWORD OVERRIDES
// ==========================================
// Explicitly define the requested passwords.
// This ensures these two accounts always use
// the exact password requested.
// ==========================================

usersMap["SANTA ROSA"] = "SANTAROSA@2026";
usersMap["SANTO TOMAS"] = "SANTOTOMAS@2026";


// ==========================================
// GET ALL USERS
// ==========================================

function getAllUsersMap() {
  return { ...usersMap };
}


// ==========================================
// NORMALIZE LOGIN USERNAME
// ==========================================

function normalizeLoginUsername(username) {
  return String(username || '')
    .trim()
    .toUpperCase();
}


// ==========================================
// CHECK USER EXISTS
// ==========================================

function userExists(cleanUser) {

  const normalizedUser = normalizeLoginUsername(cleanUser);

  if (
    normalizedUser === "DOLE4A" ||
    normalizedUser === "ADMIN"
  ) {
    return true;
  }

  return Boolean(getAllUsersMap()[normalizedUser]);
}


// ==========================================
// GET PASSWORD FOR USER
// ==========================================

function getUserPassword(username) {

  const cleanUser = normalizeLoginUsername(username);

  // Admin accounts
  if (cleanUser === "DOLE4A") {
    return null;
  }

  if (cleanUser === "ADMIN") {
    return null;
  }

  return usersMap[cleanUser] || null;
}


// ==========================================
// GET PROVINCE FOR MUNICIPALITY
// ==========================================

function getProvinceForMunicipality(muniName) {

  const cleanMuni = normalizeLoginUsername(muniName);

  for (const prov in PROVINCE_MAP) {

    if (
      Object.prototype.hasOwnProperty.call(
        PROVINCE_MAP,
        prov
      )
    ) {

      if (PROVINCE_MAP[prov][cleanMuni]) {
        return prov;
      }
    }
  }

  return "ALL";
}


// ==========================================
// GET CURRENT USER CONTEXT
// ==========================================

function getCurrentUserContext(username) {

  const cleanUser = normalizeLoginUsername(username);

  if (
    cleanUser === "DOLE4A" ||
    cleanUser === "ADMIN"
  ) {

    return {
      province: "ALL",
      municipality: "ALL",
      role: "admin"
    };
  }

  const province = getProvinceForMunicipality(cleanUser);

  return {
    province: province !== "ALL" ? province : "ALL",
    municipality: province !== "ALL" ? cleanUser : "ALL",
    role: "user"
  };
}


// ==========================================
// AUTHORIZED ADMIN EMAILS
// ==========================================

const AUTHORIZED_ADMIN_EMAILS = [
  "marco.doler4a@gmail.com",
  "ro4a_tssd.epww@dole.gov.ph",
  "tssdew.dole4a@gmail.com",
  "jobfairscalabarzon@gmail.com"
];


// ==========================================
// CHECK AUTHORIZED EMAIL
// ==========================================

function isUserAuthorized(email) {

  const cleanEmail = String(email || "")
    .trim()
    .toLowerCase();

  return AUTHORIZED_ADMIN_EMAILS.some(
    a => a.toLowerCase() === cleanEmail
  );
}


// ==========================================
// EXPORTS
// ==========================================

module.exports = {
  getAllUsersMap,
  normalizeLoginUsername,
  userExists,
  getUserPassword,
  generateUserPassword,
  getProvinceForMunicipality,
  getCurrentUserContext,
  isUserAuthorized,
  AUTHORIZED_ADMIN_EMAILS
};
