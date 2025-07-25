# Microsoft Purview REST API Integration Reference

## Authentication

### Service Principal Setup

1. **Create Service Principal:**

```bash
az ad sp create-for-rbac --name "lineage-automation-sp" --role contributor
```

1. **Assign Purview Permissions:**

```bash
az role assignment create \
  --assignee <service-principal-id> \
  --role "Purview Data Curator" \
  --scope /subscriptions/<subscription-id>/resourceGroups/<rg>/providers/Microsoft.Purview/accounts/<purview-account>
```

### Token Acquisition

```javascript
const getAccessToken = async () => {
  const response = await axios.post(
    `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`,
    new URLSearchParams({
      grant_type: 'client_credentials',
      client_id: clientId,
      client_secret: clientSecret,
      scope: 'https://purview.azure.net/.default',
    })
  );
  return response.data.access_token;
};
```

## Core API Endpoints

### Entity Bulk Create/Update

**Endpoint:** `POST /datamap/api/atlas/v2/entity/bulk`

**Headers:**

```http
Authorization: Bearer <access_token>
Content-Type: application/json
Accept: application/json
```

### Lineage Query

**Endpoint:** `GET /datamap/api/atlas/v2/lineage/{guid}`

### Search Assets

**Endpoint:** `POST /datamap/api/search/query`

## Response Codes

| Code | Description           |
| ---- | --------------------- |
| 200  | Success               |
| 201  | Created               |
| 400  | Bad Request           |
| 401  | Unauthorized          |
| 403  | Forbidden             |
| 404  | Not Found             |
| 429  | Too Many Requests     |
| 500  | Internal Server Error |
