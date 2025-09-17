// Initialize map
const map = L.map('map').setView([10.8505, 76.2711], 7); // center of Kerala

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

// Function to calculate total cases for a district and return color
function calculateSeverityColor(diseaseSummary) {
  const totalCases = Object.values(diseaseSummary)
    .reduce((sum, disease) => sum + disease.cases, 0);

  if (totalCases < 2500) return '#2ecc71';       // low risk - blue
  else if (totalCases < 4500) return '#f39c12'; // medium risk - orange
  else return '#e74c3c';                         // high risk - red
}

// Create radar marker as a divIcon
function createRadarMarker(latlng, district, diseaseSummary) {
  const color = calculateSeverityColor(diseaseSummary);

  const html = `
    <div class="radar-marker">
      <div class="radar-dot" style="background:${color}; width:12px; height:12px;"></div>
      <div class="radar-ring" style="border-color: ${color}66; width:12px; height:12px;"></div>
      <div class="radar-ring-2" style="border-color: ${color}44; width:12px; height:12px;"></div>
    </div>
  `;

  const icon = L.divIcon({
    className: '',
    html: html,
    iconSize: [36, 36],
    iconAnchor: [18, 18]
  });

  const marker = L.marker(latlng, { icon: icon });
  marker.on('click', () => fetchDistrictInfo(district));
  return marker;
}

// Load GeoJSON and add colored radar markers
fetch('kerala_districts.geojson')
  .then(res => res.json())
  .then(data => {
    data.features.forEach(feature => {
      const coords = [feature.geometry.coordinates[1], feature.geometry.coordinates[0]];
      const district = feature.properties.district;

      // Fetch disease info to decide marker color
      fetch(`http://127.0.0.1:5000/district_info?district=${district}`)
        .then(res => res.json())
        .then(data => {
          const diseaseSummary = data.disease_summary || {};
          const marker = createRadarMarker(coords, district, diseaseSummary);
          marker.addTo(map);
        })
        .catch(() => {
          const marker = createRadarMarker(coords, district, {}); // default blue if fetch fails
          marker.addTo(map);
        });
    });
  });

// Fetch district info from API and display in widget
function fetchDistrictInfo(district) {
  const infoDiv = document.getElementById('district-info');
  infoDiv.innerHTML = '<button class="close-btn" onclick="closeDistrictInfo()">&times;</button><div class="loading"></div>Loading...';
  infoDiv.classList.remove('hidden');
  
  setTimeout(() => {
    infoDiv.classList.add('show');
  }, 10);

  fetch(`http://127.0.0.1:5000/district_info?district=${district}`)
    .then(res => res.json())
    .then(data => {
      if(data.error) {
        infoDiv.innerHTML = `<button class="close-btn" onclick="closeDistrictInfo()">&times;</button><strong>${district}</strong>: ${data.error}`;
        return;
      }

      // Sort diseases by number of cases descending
      const sortedDiseases = Object.entries(data.disease_summary)
        .sort((a, b) => b[1].cases - a[1].cases);

      let html = '<button class="close-btn" onclick="closeDistrictInfo()">&times;</button>';
      html += `<h4>${district} - Disease Summary</h4>`;
      
      for(const [disease, details] of sortedDiseases) {
        html += `<b>${disease}</b>: ${details.cases} cases<br>`;
        html += `Mainly affected: ${details.mainly_affected.age_group}, ${details.mainly_affected.gender}<br>`;
        html += `Possible causes: ${details.possible_causes.join(', ')}<hr>`;
      }
      infoDiv.innerHTML = html;
    })
    .catch(err => {
      infoDiv.innerHTML = `<button class="close-btn" onclick="closeDistrictInfo()">&times;</button>Error fetching data: ${err}`;
    });
}

// Close district info panel
function closeDistrictInfo() {
  const infoDiv = document.getElementById('district-info');
  infoDiv.classList.remove('show');
  setTimeout(() => {
    infoDiv.classList.add('hidden');
  }, 400);
}

// Close on outside click
document.addEventListener('click', (e) => {
  const infoDiv = document.getElementById('district-info');
  if (!infoDiv.contains(e.target) && !e.target.closest('.leaflet-interactive')) {
    if (infoDiv.classList.contains('show')) {
      closeDistrictInfo();
    }
  }
});
