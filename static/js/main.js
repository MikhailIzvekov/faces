var url = '/_search';
var requestData = {};
var page = 1;
var persons = [];

$(document).ready(function() {
	$.post(url, requestData, function(responseData) {
		inputData(responseData);
	});
});

function collectData() {
	let data = {};
	data.person = [];
	data.page = page;
	data.q = $('#searchField').val();
	$('.person-block input:checked').each(function() {
		data.person.push($(this).attr('id'));
	});

	sendRequest(data);
}

function sendRequest(data) {
	$.post(url, data, function(responseData) {
		inputData(responseData);
	});
}

function inputData(data) {
	let allPersons = data.facets.person;
	let photo = data.hits;
	let jFacetBlock = $('#facetBlock .facets');
	let jContentBlock = $('#photoBlock .grid');

	$('#countPhoto').html('Всего: ' + data.count);

	jFacetBlock.html('');
	jContentBlock.html('');

	for (let i = 0; i < allPersons.length; i++) {
		let person = allPersons[i].toString().split(',');
		let htmlOutput = `<label><div class="person-block"><input type="checkbox" id="${person[0]}">${person[0]}<span>${person[1]}</span></div></label>`;

		jFacetBlock.append(htmlOutput);
	}

	var html = [];
	for (let i = 0; i < photo.length; i++) {
		html.push('<div class="grid-item"><img src="' + photo[i] + '"></div>');
	}
	$('#photoBlock .grid').html(html.join('\n'));

	var $grid = $('.grid').masonry({
		itemSelector: '.grid-item',
		fitWidth: true,
		columnWidth: 400,
		gutter: 5
	});
	$grid.click(function(e) {
		if ($(e.target).hasClass('grid-item')) {
			$(e.target).toggleClass('grid-item_gigante');
			$grid.masonry('layout');
		}
	});
	setTimeout(function() { $grid.masonry('layout'); }, 500);

	$('.person-block input').on('change', function() {
		if ($(this).is(':checked')) {
			persons.push($(this).attr('id'));
		} else {
			persons.splice($.inArray($(this).attr('id'), persons) ,1);
		}
		collectData();
	});

	persons.forEach(function(el) {
		$(document.getElementById(el)).prop('checked', true);
	});
}